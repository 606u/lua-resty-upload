-- Copyright (C) Yichun Zhang (agentzh)


-- local sub = string.sub
local req_socket = ngx.req.socket
local match = string.match
local setmetatable = setmetatable
local type = type
local ngx_var = ngx.var
-- local print = print


local _M = { _VERSION = '0.10' }


local CHUNK_SIZE = 4096
local MAX_LINE_SIZE = 512

local STATE_BEGIN = 1
local STATE_READING_HEADER = 2
local STATE_READING_BODY = 3
local STATE_EOF = 4


local mt = { __index = _M }

local state_handlers


local function get_boundary()
    local header = ngx_var.content_type
    if not header then
        return nil
    end

    if type(header) == "table" then
        header = header[1]
    end

    local m = match(header, ";%s*boundary=\"([^\"]+)\"")
    if m then
        return m
    end

    return match(header, ";%s*boundary=([^\",;]+)")
end


function _M.new(self, chunk_size, max_line_size)
    local boundary = get_boundary()

    -- print("boundary: ", boundary)

    if not boundary then
        return nil, "no boundary defined in Content-Type"
    end

    -- print('boundary: "', boundary, '"')

    local sock, err = req_socket()
    if not sock then
        return nil, err
    end

    local read2boundary, err = sock:receiveuntil("--" .. boundary)
    if not read2boundary then
        return nil, err
    end

    local read_line, err = sock:receiveuntil("\r\n")
    if not read_line then
        return nil, err
    end

    return setmetatable({
        sock = sock,
        size = chunk_size or CHUNK_SIZE,
        line_size = max_line_size or MAX_LINE_SIZE,
        read2boundary = read2boundary,
        read_line = read_line,
        boundary = boundary,
        state = STATE_BEGIN
    }, mt)
end


function _M.set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end


local function discard_line(self)
    local read_line = self.read_line

    local line, err = read_line(self.line_size)
    if not line then
        return nil, err
    end

    local dummy, err = read_line(1)
    if dummy then
        return nil, "line too long: " .. line .. dummy .. "..."
    end

    if err then
        return nil, err
    end

    return 1
end


local function discard_rest(self)
    local sock = self.sock
    local size = self.size

    while true do
        local dummy, err = sock:receive(size)
        if err and err ~= 'closed' then
            return nil, err
        end

        if not dummy then
            return 1
        end
    end
end


local function read_body_part(self)
    local read2boundary = self.read2boundary

    local chunk, err = read2boundary(self.size)
    if err then
        return nil, nil, err
    end

    if not chunk then
        local sock = self.sock

        local data = sock:receive(2)
        if data == "--" then
            local ok, err = discard_rest(self)
            if not ok then
                return nil, nil, err
            end

            self.state = STATE_EOF
            return "part_end"
        end

        if data ~= "\r\n" then
            local ok, err = discard_line(self)
            if not ok then
                return nil, nil, err
            end
        end

        self.state = STATE_READING_HEADER
        return "part_end"
    end

    return "body", chunk
end


local function read_header(self)
    local read_line = self.read_line

    local line, err = read_line(self.line_size)
    if err then
        return nil, nil, err
    end

    local dummy, err = read_line(1)
    if dummy then
        return nil, nil, "line too long: " .. line .. dummy .. "..."
    end

    if err then
        return nil, nil, err
    end

    -- print("read line: ", line)

    if line == "" then
        -- after the last header
        self.state = STATE_READING_BODY
        return read_body_part(self)
    end

    local key, value = match(line, "([^: \t]+)%s*:%s*(.+)")
    if not key then
        return 'header', line
    end

    return 'header', {key, value, line}
end


local function eof()
    return "eof", nil
end


function _M.read(self)
    -- local size = self.size

    local handler = state_handlers[self.state]
    if handler then
        return handler(self)
    end

    return nil, nil, "bad state: " .. self.state
end


local function read_preamble(self)
    local sock = self.sock
    if not sock then
        return nil, nil, "not initialized"
    end

    local size = self.size
    local read2boundary = self.read2boundary

    while true do
        local preamble = read2boundary(size)
        if not preamble then
            break
        end

        -- discard the preamble data chunk
        -- print("read preamble: ", preamble)
    end

    local ok, err = discard_line(self)
    if not ok then
        return nil, nil, err
    end

    local read2boundary, err = sock:receiveuntil("\r\n--" .. self.boundary)
    if not read2boundary then
        return nil, nil, err
    end

    self.read2boundary = read2boundary

    self.state = STATE_READING_HEADER
    return read_header(self)
end


function _M.delfiles(form)
    local k, v;
    for k,v in pairs(form) do
	if (v.path) then
	    os.remove(v.path);
	end
    end
    return nil;
end


-- returns a table with multipart/form-data fields
-- opts = {
--   timeout_ms = 1000,
--   files = { "field-name" = "Content-Type regex" },
--   get_filename = function(opts, field_name, file_name, content_type)
-- }
-- local form, err = upload:parse(opts);
-- ...
-- form:release();
function _M.parse(opts)
    local form, err = _M:new(chunk_size);
    if (not form) then
	return nil, err;
    end

    -- default options
    opts = opts or {};
    opts.timeout_ms = opts.timeout_ms or 1000;
    opts.upload_dir = opts.upload_dir or "/tmp/";
    -- opts.files is optional
    opts.get_filename = opts.get_filename or os.tmpname;

    form:set_timeout(opts.timeout_ms);

    local rv = {};
    setmetatable({ release = del_temp_files }, rv);
    local field_name = nil; -- filled when typ == "header"
    -- either field_name or file is used
    local field_value = nil;
    local file = nil;
    while (true) do
        local typ, res, err = form:read();
        if (not typ) then
            return _M.delfiles(rv), err;
        end

	if (typ == "header") then
	    local k, v = res[1], res[2];
	    if (k == "Content-Disposition") then
	        -- expecting 'form-data; name="<name>"[; filename="<fn.ext>"'
		if (string.sub(v, 1, 9) ~= "form-data") then
		    return _M.delfiles(rv), "Content-Disposition not starting with form-data";
		end
		local name = string.match(v, " name=\"([^\"]+)\"");
		if (not name) then
		    return _M.delfiles(rv), "Content-Disposition lacking field name";
		end
		if (field_name) then
		    return _M.delfiles(rv), "Still in field '" .. field_name .. "', but '" .. name .. "' just started";
		end
		field_name = name;

		-- test for a file upload
		name = string.match(v, " filename=\"([^\"]*)\""); -- " in name?
		if (type(name) == "nil") then
		    field_value = "";
		else
		    file = { name = name, size = 0 };
		end

	    elseif (k == "Content-Type") then
		-- expecting 'text/plain', 'image/jpeg', etc...
		if (not field_name) then
		    return _M.delfiles(rv), "got Content-Type without Content-Disposition";
		end
		if (string.find(v, "/", 1, true) == nil) then
		    return _M.delfiles(rv), "unexpected Content-Type '" .. v .. "'";
		end
		if (not file) then
		    return _M.delfiles(rv), "got Content-Type for a form field";
		end
		if (opts.files) then
		    local re = opts.files[field_name];
		    if (not re) then
			return _M.delfiles(rv), "file '" .. field_name .. "' were unexpected";
		    end
		    if (not string.match(v, re)) then
			return _M.delfiles(rv), "Content-Type '" .. v .. "' for '" .. field_name .. "' does not match to '" .. re .. "'";
		    end
		end
		file.type = v;
		file.path = opts.get_filename(opts, field_name, file.name, v);
		file.fd = io.open(file.path, "w+");
		if (not file.fd) then
		    return _M.delfiles(rv), "failed to create file '" .. file.path .. "'";
		end
	    end

	elseif (typ == "body") then
	    if (not file and not field_value) then
		return _M.delfiles(rv), "got body without header";
	    end
	    if (file) then
		file.fd:write(res);
		file.size = file.size + string.len(res);
	    else -- a form field
		field_value = field_value .. res;
	    end

	elseif (typ == "part_end") then
	    if (not field_name) then
		return _M.delfiles(rv), "got part_end without header";
	    end
	    local v;
	    if (file) then
		file.fd:close();
		file.fd = nil;
		v = file;
	    else -- a form field
		v = field_value;
	    end
	    local x = rv[field_name];
	    if (not x) then
		rv[field_name] = v;
	    elseif (type(x) == "table") then
		table.insert(x, v);
	    else
		rv[field_name] = { x, v };
	    end
	    field_name = nil;
	    field_value = nil;
	    file = nil;

	elseif (typ == "eof") then
	    if (field_name) then
		return _M.delfiles(rv), "got eof without last part_end";
	    end
	    return rv, false;

	else
	    return nil, "bad type '" .. typ .. "' returned";
        end
    end
end


state_handlers = {
    read_preamble,
    read_header,
    read_body_part,
    eof
}


return _M
