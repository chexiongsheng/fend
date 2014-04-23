local ffi = require "ffi"
local bit = require "bit"
require "fend.common"
include "stdio"
include "fcntl"
include "unistd"

ffi.cdef [[
	typedef struct {
		const int fd;
		bool no_close:1; // Should the file handle be closed on collection?
	} file_t;
]]

local new = ffi.typeof ( "file_t" )
local file_methods = { }
local file_mt = {
	__index = file_methods ;
	__tostring = function ( self )
		return "file(" .. tostring(self:getfd()) .. ")"
	end ;
	__gc = function ( self )
        if not self.no_close then --原来的gc，如果fd异常会导致程序退出，fd异常比较常见？比如zookeeper的服务端关闭连接，这个时候操作fd全部返回“Bad file descriptor”
            ffi.C.close ( self:getfd())
        end
	end ;
}

function file_methods:close ( )
	if not self.no_close then
		if ffi.C.close ( self:getfd() ) == -1 then
			error ( ffi.string ( ffi.C.strerror ( ffi.errno ( ) ) ) )
		end
		self.no_close = true
	end
end
function file_methods:getfd ( )
	return self.fd
end

function file_methods:read ( buff , len )
	local c = tonumber ( ffi.C.read ( self:getfd() , buff , len ) )
	if c == 0 then
		return nil , "EOF"
	elseif c == -1 then
		local err = ffi.errno ( )
		if err == defines.EAGAIN or err == defines.EWOULDBLOCK then
			return 0
		else
			return nil , ffi.string ( ffi.C.strerror ( err ) )
		end
	end
	return c
end

function file_methods:write ( buff , len )
	if not ffi.istype("char*",buff) then
		buff = tostring ( buff )
	end
	len = len or #buff
	local c = tonumber ( ffi.C.write ( self:getfd() , buff , len ) )
	if c == -1 then
		local err = ffi.errno ( )
		if err == defines.EAGAIN or err == defines.EWOULDBLOCK then
			return 0
		else
			return nil , ffi.string ( ffi.C.strerror ( err ) )
		end
	end
	return c
end

function file_methods:set_blocking ( bool )
	local flags = ffi.C.fcntl ( self:getfd() , defines.F_GETFL )
	if flags == -1 then
		error ( ffi.string ( ffi.C.strerror ( ffi.errno ( ) ) ) )
	end
	if not bool then
		flags = bit.bor ( flags , defines.O_NONBLOCK )
	else
		flags = bit.band ( flags , bit.bnot ( defines.O_NONBLOCK ) )
	end
	if ffi.C.fcntl ( self:getfd() , defines.F_SETFL , ffi.cast ( "int" , flags ) ) == -1 then
		error ( ffi.string ( ffi.C.strerror ( ffi.errno ( ) ) ) )
	end
end

ffi.metatype ( "file_t" , file_mt )
local raw_fd_map = setmetatable({}, {__mode = "v"})
local function wrap ( fd , no_close )
    if raw_fd_map[fd] then
        return raw_fd_map[fd]
    end
	local is_luafile = io.type ( fd )
	if is_luafile then
		fd = ffi.C.fileno ( fd )
		if fd == -1 then
			error ( ffi.string ( ffi.C.strerror ( ffi.errno ( ) ) ) )
		end
	end
	local file =  new ( { fd = fd , no_close = no_close or (is_luafile == "closed file") } ) -- COMPAT: Wrap in table for luaffi
    raw_fd_map[fd] = file
    return file
end

return {
	wrap = wrap ;
	file_mt = file_mt ;
}
