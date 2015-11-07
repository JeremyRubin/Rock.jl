
using Rock
using Base.Test
w = addprocs(3)
@everywhere module KVStore
import Rock
@enum Op Put Append
type Command <: Rock.ExternalCriticalCommand
    op::Op
    k::ASCIIString
    v::ASCIIString
    Command(op, k,v) = new(op, k,v)
end
typealias Snapshot Dict{ASCIIString, ASCIIString}
function transition(px, t)
    if t.op == Put
        px.snapshot[t.k] = t.v
    elseif t.op == Append
        try
            px.snapshot[t.k] = "$(px.snapshot[t.k])$(t.v)"
        catch KeyError
            px.snapshot[t.k] = t.v
        end
        
    end
end
function mk(port, peers::Array{Tuple{ASCIIString, Int64},1}, path::ASCIIString)
    slot_dir = "$(path)/slots"
    forget_f = "$(path)/forget.txt"
    snapshot_f = "$(path)/snapshot.txt"
    slot_num_f = "$(path)/slot_num.txt"
    ctx = Rock.Context(port, peers, transition, slot_dir, forget_f, snapshot_f, slot_num_f)
    Rock.node(ctx,  port-8080, Snapshot())
end
function command(h, p, c)
    Rock.command(Rock.client("localhost", 8081), c)
end
function append(h,p,k,v)
    command(h,p, Command(Append, k, v))
end

function put(h,p,k,v)
    command(h,p, Command(Put, k, v))
end
end

import KVStore
peers = [("localhost", 8081), ("localhost", 8082), ("localhost", 8083) ]::Array{Tuple{ASCIIString, Int64},1}
for (i, worker) = enumerate(w)
    @spawnat worker (mktempdir(".") do f
                     KVStore.mk(8080+i, peers, f)
                     end)
end
wait(Timer(1))
KVStore.put("localhost",8081, "Test", "Put")
# write your own tests here
@test 1 == 1
