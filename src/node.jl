include("Locks.jl")
include("Rock.jl")
module KVStore
import Rock
@enum Op Put Append NoOp
type Command
    op::Op
    k::ASCIIString
    v::ASCIIString
    Command() = new(NoOp, "", "")
    Command(op, k,v) = new(op, k,v)
end
typealias Snapshot Dict{ASCIIString, ASCIIString}
function transition(d, t)
    if t.op == Put
        d[t.k] = t.v
    elseif t.op == Append
        try
            d[t.k] = "$(d[t.k])$(t.v)"
        catch KeyError
            d[t.k] = t.v
        end
        
    end
end
function mk(port, peers::Array{Tuple{ASCIIString, Int64},1})
    slot_dir = "$(ENV["ROCK_PATH"])/slots"
    forget_f = "$(ENV["ROCK_PATH"])/forget.txt"
    snapshot_f = "$(ENV["ROCK_PATH"])/snapshot.txt"
    slot_num_f = "$(ENV["ROCK_PATH"])/slot_num.txt"
    ctx = Rock.Context(Command, Snapshot, port, peers, transition, slot_dir, forget_f, snapshot_f, slot_num_f)
    Rock.node(ctx,  port-8080, Snapshot())
end
function command(h, p, c)
    Rock.command(Rock.client(Command, "localhost", 8081), c)
end
function append(h,p,k,v)
    command(h,p, Command(Append, k, v))
end

function put(h,p,k,v)
    command(h,p, Command(Put, k, v))
end
end

import KVStore
port = parse(Int64,ENV["ROCK_PORT"])
peers = [("localhost", 8081), ("localhost", 8082), ("localhost", 8083) ]::Array{Tuple{ASCIIString, Int64},1}
if port  > 8080
    KVStore.mk(port, peers)
else
    KVStore.append("localhost",8081, "HELLO", "WORLD")
end


