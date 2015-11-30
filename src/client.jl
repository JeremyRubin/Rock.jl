module Client
import Rock
using Logging
@Logging.configure(level=DEBUG)
type t
    host::ASCIIString
    port::Int64
end

function make(host::ASCIIString, port::Int64)
    t(host, port)
end
function _connect(c::t)
    connect(c.host, c.port)
end
function command(c::t, cmd::Rock.RPC.Command)
    @debug "Issuing Rock Command ($cmd) to $c"
    conn = _connect(c)
    try
        serialize(conn, cmd)
        flush(conn)
        deserialize(conn)
    catch err
        rethrow(err)
    finally
        close(conn)
    end
end


end
