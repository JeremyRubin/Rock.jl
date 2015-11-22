
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
        SQLite.query(Rock.db(px), "insert into snapshot values (?,?)",[t.k,t.v])
    elseif t.op == Append
        SQLite.query(Rock.db(px),"insert into snapshot values (?, ?) on duplicate key update snapshot set V = V||? where K = ?", [t.k, t.v, t.v, t.k])
    end
end
function mk(port, peers::Array{Tuple{ASCIIString, Int64},1}, path::ASCIIString)
    ctx = Rock.Context(port, peers, transition, "$path/sqlite.db")
    Rock.node(ctx,  port-8080)
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
