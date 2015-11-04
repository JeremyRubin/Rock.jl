module Rock
export ServerId
typealias ServerId Int64
typealias  LogIndex Int64
typealias Term Int64
typealias Prepare Int64
typealias SlotNum Int
@enum Fate Decided Pending Forgotten
abstract AbstractCommand
    
function no_op(t::Type{AbstractCommand})
end

type ExtraInfo
    completed::Array{Int64, 1}
end
type PrepareArgs
    slot_num::SlotNum
    n::Int64
    info::ExtraInfo
end
type PrepareReply{Command <: AbstractCommand}
    slot_num::SlotNum
    Ok::Bool
    n::Int64
    n_a::Int64
    v_a::Nullable{Command}
    info::ExtraInfo
end
type AcceptArgs{Command<: AbstractCommand}
    slot_num::SlotNum
    n::Int64
    v::Command
    info::ExtraInfo
end
type AcceptReply{Command<: AbstractCommand}
    slot_num::SlotNum
    Ok::Bool
    n::Int64
    v::Command
    info::ExtraInfo
end
type DecideArgs{Command<: AbstractCommand}
    slot_num::SlotNum
    v::Command
    info::ExtraInfo
end
type DecideReply
    slot_num::SlotNum
    Ok::Bool
    info::ExtraInfo
end


type Slot{Command<: AbstractCommand}
    num::SlotNum
    n_p::Int64
    n_a::Int64
    v_a::Nullable{Command}
    fate::Fate
end
NULL_SLOT = Slot(0,0,0,Nullable(), Pending)
FORGOTTEN_SLOT = Slot(0,0,0,Nullable(), Forgotten)

type Log{Command<: AbstractCommand}
    slots::Dict{SlotNum, Slot{Command}}
    maxSlot::Int
end



type Context{T, S}
    t::Type{T}
    s::Type{S}
    no_op::Function
    transition::Function
    slot_dir
    forget_f
end
type Paxos{Command<: AbstractCommand, Snapshot}
    self::Int
    peers::Array{Tuple{AbstractString, Int},1}
    connPool::Array{Nullable{TCPSocket}, 1}
    log::Log{Command}
    lock::Base.Semaphore
    completed::Array{Int64, 1}
    snapshot::Snapshot
    forgot::Int64
    context::Context{Command, Snapshot}
end

function getExtraInfo{C,S}(px::Paxos{C,S})
    holding(px.lock) do
        ExtraInfo(px.completed)
    end
end
function message(px, m, chan, i)
    # if isnull(px.connPool[i])
    #     host, port = px.peers[i]
    #     px.connPool[i] = Nullable(connect(host, port))
    # end
    host, port = px.peers[i]
    io = connect(host, port)
    serialize(io, m)
    flush(io)
    val = deserialize(io)
    close(io)
    if !isnull(chan)
        put!(chan.value, val)
    end
    try
        @async Forgettor(px, px.context.slot_dir, px.context.forget_f, val.info.completed)
    catch err
        print(err)
    end
end
function broadcast{C,S}(px::Paxos{C,S}, m, chan)
    @async for i = 1:length(px.peers)
        message(px, m, Nullable(chan), i)
    end
end
function broadcast{C,S}(px::Paxos{C,S}, m)
    @async for i = 1:length(px.peers)
        message(px, m, Nullable(),i)
    end
end


function getMaxSlot{Command}(l::Log{Command})
    l.maxSlot
end
function getSlot!{Command, Snapshot}(px::Paxos{Command, Snapshot}, s::SlotNum)
    if s <= px.forgot
        FORGOTTEN_SLOT
    else
        l = px.log    
        try
            l.slots[s]
        catch KeyError
            l.slots[s] = Slot(s, 0,0,Nullable{Command}(), Pending)
            l.maxSlot = max(l.maxSlot, s)
            getSlot!(px, s)
        end
    end
end

function getSlot{Command, Snapshot}(px::Paxos{Command, Snapshot}, s::SlotNum)
    if s <= px.forgot
        Nullable(FORGOTTEN_SLOT)
    else
        l = px.log    
        try
            Nullable(l.slots[s])
        catch KeyError
            Nullable()
        end
    end
end
function holding(f::Function, sem::Base.Semaphore)
    Base.acquire(sem)
    f_ = f()
    Base.release(sem)
    f_
end

function persistently(f::Function,  slot::Slot)
    open("$(ENV["ROCK_PATH"])/slots/$(slot.num)", "w+") do io
        f()
        serialize(io, slot)
        
    end
end
function Handle{Command,Snapshot}(px::Paxos{Command,Snapshot},args::PrepareArgs)
    holding(px.lock) do
        s = getSlot!(px,args.slot_num)
        if args.n > s.n_p
            persistently(s) do
                s.n_p = args.n
            end
            PrepareReply(args.slot_num, true, s.n_p, s.n_a, s.v_a, ExtraInfo(px.completed))
        else
            PrepareReply(args.slot_num, false, s.n_p, s.n_a, s.v_a, ExtraInfo(px.completed))
        end

    end
end

function Handle{Command, Snapshot}(px::Paxos{Command, Snapshot}, args::AcceptArgs{Command})
    holding(px.lock) do
        s = getSlot!(px, args.slot_num)
        if args.n >= s.n_p
            persistently(s) do
                s.n_p = args.n
                s.n_a = args.n
                s.v_a = Nullable(args.v)
            end
            AcceptReply(args.slot_num, true, s.n_p, args.v, ExtraInfo(px.completed))
        else
            AcceptReply(args.slot_num, false, 0, px.context.no_op(Command), ExtraInfo( px.completed))
        end

    end
end
function Handle{Command, Snapshot}(px::Paxos{Command, Snapshot}, args::DecideArgs{Command})

    holding(px.lock) do
        s = getSlot!(px, args.slot_num)
        persistently(s) do
            s.v_a = Nullable(args.v)
            s.fate = Decided
        end
        DecideReply(args.slot_num, true, ExtraInfo(px.completed))
    end
end

function acceptPhase{Command, Snapshot}(px::Paxos{Command, Snapshot},slot_num::SlotNum, n::Int64, v::Command, majority)
    accept = AcceptArgs(slot_num, n, v, getExtraInfo(px))
    chan = Base.Channel{AcceptReply}(5)
    broadcast(px, accept, chan)
    # @async for (i,(host,port)) = enumerate(px.peers)
    #     io = connect(host, port) 
    #     serialize(io, accept)
    #     flush(io)
    #     put!(chan, deserialize(io))
    #     close(io)
    # end
    positives = 0
    negatives = 0
    while true
        r = take!(chan)
        
        if r.Ok
            positives += 1
            if positives >= majority
                decide = DecideArgs(slot_num, v, getExtraInfo(px))
                broadcast(px, decide)
                # @async for (i,(host,port)) = enumerate(px.peers)
                #     io = connect(host, port) 
                #     serialize(io, decide)
                #     flush(io)
                #     # put!(chan, deserialize(io)) # Maybe later process the decide replies
                #     close(io)
                # end
                return true
            end


        else
            negatives +=1
            if negatives >= majority
                return false
            end
        end

    end
end

function proposer{Command, Snapshot}(px::Paxos{Command, Snapshot}, v::Command, slot_num::SlotNum)
    epoch = -1
    majority = div(length(px.peers) +2, 2)
    while (holding(px.lock) do
           getSlot!(px, slot_num).fate == Pending
           end)
        epoch += 1
        n = length(px.peers)*epoch + px.self + 1
        chan = Base.Channel{PrepareReply}(5)# Magic number, not super important! Could be 1
        prep = PrepareArgs(slot_num, n, getExtraInfo(px))
        broadcast(px, prep, chan)
        # @async for (i,(host,port)) = enumerate(px.peers)
        #     io =connect(host, port)
        #     serialize(io, prep)
        #     flush(io)
        #     put!(chan, deserialize(io))
        #     close(io)
        # end
        positives = 0
        negatives = 0
        n_a = 0
        v_a = v
        loop = true
        while loop
            r = take!(chan)
            if r.Ok
                positives += 1
                if r.n_a > n_a
                    if !isnull(r.v_a)
                        v_a = r.v_a.value
                    end
                end
                if positives >= majority
                    acceptPhase(px, slot_num, n, v_a, majority)
                    loop = false
                end
            else

                negatives += 1
            end
            if negatives >= majority
                loop = false
            end
        end

    end
    
end
function Handle{Command, Snapshot}(px::Paxos{Command, Snapshot}, args::Command)
    slot_num = 0
    holding(px.lock) do
        slot_num = getMaxSlot(px.log)+1
    end
    @async proposer(px, args, slot_num)
    slot_num
    
end



function RPCServer{Command, Snapshot}(px::Paxos{Command, Snapshot}, port::Int, slot_dir, forget_f)
    server = listen(port)
    while true
        conn = accept(server)
        @async begin
            try
                while true
                    args = deserialize(conn)
                    @show args
                    r = Handle(px, args)
                    serialize(conn,r)
                    try
                        @async Forgettor(px, slot_dir, forget_f, args.info.completed)
                    catch err
                        print(err)
                    end
                end
            catch err
                # print("Connection Failed With $err on node $(px.self)")

            end
        end
    end
end
function LogCrawler{Command, Snapshot}(px::Paxos{Command, Snapshot}, f, completed_f, snapshot_f)
    times = 0
    proposer_launched = false
    while true
        holding(px.lock) do
            changed = false
            while true
                next = getSlot(px, px.completed[px.self]+1)
                if isnull(next)
                    break
                elseif next.value.fate == Decided
                    proposer_launched = false
                    times = 0
                    px.completed[px.self] += 1
                    if !isnull(next.value.v_a)
                        f(px.snapshot, next.value.v_a.value)
                        changed = true
                    end
                else
                    if !proposer_launched && next.value.num != getMaxSlot(px.log)
                        times +=1
                        if times > 1000 
                            proposer_launched = true
                            slot_num = getMaxSlot(px.log)+1
                            @async proposer(px, px.context.no_op(Command), slot_num)
                        end
                    end

                    break
                end

            end
            if changed
                open(completed_f, "w") do io
                    serialize(io, px.completed[px.self])
                end

                open(snapshot_f, "w") do io
                    @show px.snapshot
                    serialize(io, px.snapshot)
                end
            end

        end
        wait(Timer(0.01))
    end
end
function Forgettor{Command, Snapshot}(px::Paxos{Command, Snapshot}, slot_dir, forget_f, completed::Array{Int64, 1})
    print("enter Forgettor\n")
    @show px.completed
    @show completed
    holding(px.lock) do
        px.completed = max(px.completed, completed)
        @show px.completed
        px.forgot = min(px.completed...)

        for f=readdir(slot_dir)
            if parse(Int64, f) <= px.forgot
                rm("$slot_dir/$f")
            end
        end

        open(forget_f, "w") do io
            serialize(io, px.forgot)
        end

    end
end

function node{Command, Snapshot}(context::Context{Command, Snapshot}, port::Int64, peers::Array{Tuple{ASCIIString, Int64},1}, self::Int64, snapshot::Snapshot)
    print("Starting on $port\n")
    
    slot_dir = context.slot_dir
    mkpath(slot_dir)
    log = Dict{SlotNum, Slot{context.t}}()
    m = -1
    for f=readdir(slot_dir)
        open("$slot_dir/$f") do io
            s = deserialize(io)
            log[s.num] = s
            m = max(m,s.num)
        end
    end

    snapshot_file = "$(ENV["ROCK_PATH"])/snapshot.txt"
    if isfile(snapshot_file)
        open(snapshot_file, "w+") do io
            snapshot = deserialize(io)
        end
    end

    slot_num_file = "$(ENV["ROCK_PATH"])/slot_num.txt"
    slot_nums = fill(-1,length(peers))
    if isfile(slot_num_file)
        open(slot_num_file, "w+") do io
            slot_nums[self] = deserialize(io)
        end
    end

    forget_f = context.forget_f
    forget = -1

    if isfile(forget_f)
        open(forget_f, "w") do io
            forget = deserialize(io)
        end
    end
    conns = Array(Nullable{TCPSocket}, length(peers))
    fill!(conns, Nullable())
    px = Paxos{context.t, context.s}(self, peers, conns, Log{context.t}(log, m), Base.Semaphore(1), slot_nums, snapshot, forget, context)
    @async RPCServer(px, port, slot_dir, forget_f)

    LogCrawler(px,context.transition, slot_num_file, snapshot_file)
    wait(Timer(3))

end
type Client{Command}
    t::Type{Command}
    conn
end

function client{Command}(t::Type{Command}, host::AbstractString, port::Int64)
    Client(t, connect(host, port))
end
function command{Command}(c::Client{Command}, cmd::Command)

    serialize(c.conn, cmd)
    flush(c.conn)
    deserialize(c.conn)
    
end


end


module RockInstance
using Rock
@enum Op Put Append NoOp
type Command <: Rock.AbstractCommand
    op::Op
    k::ASCIIString
    v::ASCIIString
end

function no_op(t::Type{Command})
    Command(NoOp, "", "")
end
    
function f(d, t)
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
typealias Snapshot Dict{ASCIIString, ASCIIString}


function mk(port, peers)
    slot_dir = "$(ENV["ROCK_PATH"])/slots"
    forget_f = "$(ENV["ROCK_PATH"])/forget.txt"
    Rock.node(Rock.Context(Command, Snapshot, no_op, f, slot_dir, forget_f ), port, peers, port-8080, Snapshot())
end

end

import RockInstance
port = parse(Int64,ENV["ROCK_PORT"])
peers = [("localhost", 8081), ("localhost", 8082), ("localhost", 8083) ]::Array{Tuple{ASCIIString, Int64},1}
if port  > 8080
    RockInstance.mk(port, peers)
else
    RockInstance.Rock.command(RockInstance.Rock.client(RockInstance.Command, "localhost", 8081), RockInstance.Command(RockInstance.Append, "a", "x"))
end


