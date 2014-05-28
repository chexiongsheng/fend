--
--------------------------------------------------------------------------------
--         FILE:  socket_ext.lua
--        USAGE:  ./socket_ext.lua 
--  DESCRIPTION:  
--      OPTIONS:  ---
-- REQUIREMENTS:  ---
--         BUGS:  ---
--        NOTES:  ---
--       AUTHOR:  John (J), <chexiongsheng@qq.com>
--      COMPANY:  
--      VERSION:  1.0
--      CREATED:  2014年05月26日 15时51分56秒 CST
--     REVISION:  ---
--------------------------------------------------------------------------------
--
local threadpool = require 'threadpool_ext'
local ffi = require "ffi"
local fend_socket = require "fend.socket"
require "fend.common"
include "errno"
include "sys/socket"

local sock_methods = fend_socket.socket_mt.__index
local getsockerr   = fend_socket.getsockerr

function sock_methods:connect ( ... )
	local sockaddr , sockaddr_len
	if ffi.istype ( "struct addrinfo*" , (...) ) then
		local addrinfo
		addrinfo  = ...
		sockaddr , sockaddr_len = addrinfo.ai_addr , addrinfo.ai_addrlen
	else
		sockaddr , sockaddr_len = ...
	end

	local rc = ffi.C.connect ( self:getfd() , sockaddr , sockaddr_len )
	if rc  == -1 then
		local err = ffi.errno ( )
		if err ~= defines.EINPROGRESS then
			return rc , ffi.string ( ffi.C.strerror ( err ) )
        end
    end
    return rc
end

sock_methods._recv = sock_methods.recv
function sock_methods:__recv (buff, len, flags, timeout, fill_all)
    assert(not fill_all or self.co_blocking, 'fill is only avaiable in co_blocking mode!')
    if not self.co_blocking then return self:_recv(buff, len, flags) end
    if self.sync then
        self.__recv_cs:enter()
    else
        assert(self.__recv_tid == nil, 'another thread recving!')
        self.__recv_tid = threadpool.running.id
    end

	local got, rerr = 0
	while true do
		local n , err = self:_recv ( buff+got , len-got, flags)
		if not n then
            got, rerr = n , err
            break
        else
            if n > 0 and not fill_all then
                got = n
                break
            end
		end
		got = got + n
		if got >= len then break end
        local rc = threadpool.wait(self, timeout or self.co_blocking_timeout)
        if self.is_closed then
            rerr = self.closed_reason
            break
        end
        if rc ~= 0 then
            rerr = rc == threadpool.TIMEOUT and "timeout" or "unknown error, rc="..rc
            break
        end
    end
    if self.sync then
        self.__recv_cs:leave()
    else
        self.__recv_tid = nil
    end
	return got, rerr
end
function sock_methods:recv(buff, len, flags, timeout)
    return self:__recv(buff, len, flags, timeout)
end
function sock_methods:fill(buff, len, flags, timeout)
    return self:__recv(buff, len, flags, timeout, "fill")
end
sock_methods.receive = sock_methods.recv

sock_methods._send = sock_methods.send
function sock_methods:send(buff , len , flags, timeout)
    if not self.co_blocking then return self:_send(buff, len, flags) end
    if self.sync then
        self.__send_cs:enter()
    else
        assert(self.__send_tid == nil, 'another thread sending!')
        self.__send_tid = threadpool.running.id
    end

	if not ffi.istype ( "char*" , buff ) then
		len = len or #buff
		buff = ffi.cast ( "const char*" , buff )
	end
	local sent, rerr = 0
	while true do
		local n , err = self:_send ( buff+sent , len-sent )
		if not n then
            sent, rerr = n , err
            break
		end
		sent = sent + n
		if sent >= len then break end
        local rc = threadpool.wait(self, timeout or self.co_blocking_timeout)
        if self.is_closed then
            rerr = self.closed_reason
            break
        end
        if rc ~= 0 then
            rerr = rc == threadpool.TIMEOUT and "timeout" or "unknown error, rc="..rc
            break
        end
	end
    if self.sync then
        self.__send_cs:leave()
    else
        self.__send_tid = nil
    end
	return sent, rerr
end

sock_methods._accept = sock_methods.accept
function sock_methods:accept(with_sockaddr, timeout)
    if not self.co_blocking then return self:_accept(buff, len, flags) end
    if self.sync then
        self.__recv_cs:enter()
    else
        assert(self.__recv_tid == nil, 'another thread recving!')
        self.__recv_tid = threadpool.running.id
    end

    local client , sockaddr , sockaddr_len
	while true do
		client , sockaddr , sockaddr_len = self:_accept (with_sockaddr)
		if client then
			break
        end
        local rc = threadpool.wait(self, timeout or self.co_blocking_timeout)
        if self.is_closed then
            sockaddr = self.closed_reason
            break
        end
        if rc ~= 0 then
            sockaddr = rc == threadpool.TIMEOUT and "timeout" or "unknown error, rc="..rc
            break
        end
    end

    if self.sync then
        self.__recv_cs:leave()
    else
        self.__recv_tid = nil
    end
    return client , sockaddr , sockaddr_len
end

local function notify_sending_thread(sock)
    local sending_thread_id
    if sock.sync then
        sock.__send_cs:entered_thread()
    else
        sending_thread_id = sock.__send_tid 
    end
    if sending_thread_id then
        threadpool.notify(sending_thread_id, sock, 0)
    end
end

local function notify_recving_thread(sock)
    local recving_thread_id
    if sock.sync then
        recving_thread_id = sock.__recv_cs:entered_thread()
    else 
        recving_thread_id = sock.__recv_tid
    end
    if recving_thread_id then
        threadpool.notify(recving_thread_id, sock, 0)
    end
end

local handle_close = function(sock, closed_reason)
    sock.is_closed = true
    sock.closed_reason = closed_reason
    notify_sending_thread(sock)
    notify_recving_thread(sock)
end

local co_blocking_cbs = {
    write = function(sock)
        notify_sending_thread(sock)
    end;
    read = function(sock)
        notify_recving_thread(sock)
    end;
    close = function(sock)
        handle_close(sock, 'close')
    end;
    error = function(sock)
        handle_close(sock, ffi.string ( ffi.C.strerror ( ffi.errno ( ) ) ))
    end;
}

function sock_methods:set_co_blocking(epoll_obj, timeout, sync)
    self.co_blocking = not not epoll_obj
    self.co_blocking_timeout = timeout or 100
    self.sync = sync
    if self.co_blocking then
        if not sync then
            self.__recv_cs, self.__send_cs = threadpool.new_critical_section(), threadpool.new_critical_section()
        end
        epoll_obj:add_fd(self, co_blocking_cbs)
    else
        self.__recv_cs, self.__send_cs = nil, nil
        epoll_obj:del_fd(self)
    end
end


