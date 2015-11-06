module Rock
using Locks
typealias ServerId Int64
typealias  LogIndex Int64
typealias Term Int64
typealias Prepare Int64
typealias SlotNum Int
@enum Fate Decided Pending Forgotten
type Slot{Command}
    num::SlotNum
    n_p::Int64
    n_a::Int64
    v_a::Nullable{Command}
    fate::Fate
    lock::Lock
    condition::LevelTrigger
    # Slot(num, n_p, n_a, v_a, fate) = new(num, n_p, n_a, v_a, fate, Lock(), LevelTrigger())
end
NULL_SLOT      = Slot(0,0,0,Nullable(), Pending, Lock(), LevelTrigger())
FORGOTTEN_SLOT = Slot(0,0,0,Nullable(), Forgotten, Lock(), LevelTrigger())
type ExtraInfo
    completed::Array{Int64, 1}
end
type PrepareArgs
    slot_num::SlotNum
    n::Int64
    info::ExtraInfo
end
type PrepareReply{Command}
    slot_num::SlotNum
    Ok::Bool
    n::Int64
    n_a::Int64
    v_a::Nullable{Command}
    info::ExtraInfo
end
type AcceptArgs{Command}
    slot_num::SlotNum
    n::Int64
    v::Command
    info::ExtraInfo
end
type AcceptReply{Command}
    slot_num::SlotNum
    Ok::Bool
    n::Int64
    v::Command
    info::ExtraInfo
end
type DecideArgs{Command}
    slot_num::SlotNum
    v::Command
    info::ExtraInfo
end
type DecideReply
    slot_num::SlotNum
    Ok::Bool
    info::ExtraInfo
end



type Log{Command}
    slots::Dict{SlotNum, Slot{Command}}
    maxSlot::Int
end



type Context{T, S}
    t::Type{T}
    s::Type{S}
    port::Int64
    peers::Array{Tuple{ASCIIString, Int64},1}
    transition::Function
    slot_dir
    forget_f
    snapshot_f
    slot_num_f
end
type Paxos{Command, Snapshot}
    self::Int
    log::Log{Command}
    completed::Array{Int64, 1}
    snapshot::Snapshot
    forgot::Int64
    context::Context{Command, Snapshot}
    lock::Lock
    forgetChan::Channel{Array{Int64, 1}}
    Paxos(self, log, completed, snapshot, forgot, context) = new(self, log, completed, snapshot, forgot, context, Lock(), Channel{Array{Int64, 1}}(5))
end

function getExtraInfo{C,S}(px::Paxos{C,S})
    holding(px.lock, "getExtraInfo") do
        ExtraInfo(px.completed)
    end
end
function message(px, m, chan, i)
    host, port = px.context.peers[i]
    io = connect(host, port)
    serialize(io, m)
    flush(io)
    val = deserialize(io)
    close(io)
    if !isnull(chan)
        put!(chan.value, val)
    end
    try
        put!(px.forgetChan, val.info.completed)
        # @async Forgettor(px, val.info.completed)
    catch err
        print(err)
    end
end
function broadcast{C,S}(px::Paxos{C,S}, m, chan)
    @async for i = 1:length(px.context.peers)
        message(px, m, Nullable(chan), i)
    end
end
function broadcast{C,S}(px::Paxos{C,S}, m)
    @async for i = 1:length(px.context.peers)
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
            l.slots[s] = Slot(s, 0,0,Nullable{Command}(), Pending, Lock(), LevelTrigger())
            l.maxSlot = max(l.maxSlot, s)
            getSlot!(px, s)
        end
    end
end
function safe_getSlot!{Command, Snapshot}(px::Paxos{Command, Snapshot}, s::SlotNum)
    holding(px.lock, "safe_getSlot! $(s)") do
        getSlot!(px, s)
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
function safe_getSlot{Command, Snapshot}(px::Paxos{Command, Snapshot}, s::SlotNum)
    holding(px.lock, "safe_getSlot $(s)") do
        getSlot(px, s)
    end
end

function persistently(f::Function,  slot::Slot)
    open("$(ENV["ROCK_PATH"])/slots/$(slot.num)", "w+") do io
        f()
        serialize(io, slot)
        
    end
end
function Handle{Command,Snapshot}(px::Paxos{Command,Snapshot},args::PrepareArgs)
    s = safe_getSlot!(px,args.slot_num)
    holding(s.lock, "HandlePrepare $(s.num)") do
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
    s = safe_getSlot!(px, args.slot_num)
    holding(s.lock, "HandleAccept $(s.num)") do
        if args.n >= s.n_p
            persistently(s) do
                s.n_p = args.n
                s.n_a = args.n
                s.v_a = Nullable(args.v)
            end
            AcceptReply(args.slot_num, true, s.n_p, args.v, ExtraInfo(px.completed))
        else
            AcceptReply(args.slot_num, false, 0, Command(), ExtraInfo( px.completed))
        end

    end
end
function Handle{Command, Snapshot}(px::Paxos{Command, Snapshot}, args::DecideArgs{Command})
    s = safe_getSlot!(px, args.slot_num)
    holding(s.lock, "$(s.num)") do
        if s.fate != Decided
            persistently(s) do
                s.v_a = Nullable(args.v)
                s.fate = Decided
            end
            @async signal(s.condition)
        end
    end
    DecideReply(args.slot_num, true, ExtraInfo(px.completed))
end

function acceptPhase{Command, Snapshot}(px::Paxos{Command, Snapshot},slot_num::SlotNum, n::Int64, v::Command, majority)
    accept = AcceptArgs(slot_num, n, v, getExtraInfo(px))
    chan = Base.Channel{AcceptReply}(5)
    broadcast(px, accept, chan)
    positives = 0
    negatives = 0
    while true
        r = take!(chan)
        
        if r.Ok
            positives += 1
            if positives >= majority
                decide = DecideArgs(slot_num, v, getExtraInfo(px))
                broadcast(px, decide)
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
    majority = div(length(px.context.peers) +2, 2)
    while (holding(px.lock, "Proposer While Loop $slot_num") do
           getSlot!(px, slot_num).fate == Pending
           end)
        epoch += 1
        n = length(px.context.peers)*epoch + px.self + 1
        chan = Base.Channel{PrepareReply}(5)# Magic number, not super important! Could be 1
        prep = PrepareArgs(slot_num, n, getExtraInfo(px))
        broadcast(px, prep, chan)
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
    slot_num =  holding(px.lock, "Handle Command") do
        getMaxSlot(px.log)+1
    end
    @async proposer(px, args, slot_num)
    slot_num
    
end



function RPCServer{Command, Snapshot}(px::Paxos{Command, Snapshot})
    server = listen(px.context.port)
    while true
        conn = accept(server)
        @async begin
            try
                while true
                    args = deserialize(conn)
                    # @show args
                    r = Handle(px, args)
                    serialize(conn,r)
                    try
                        put!(px.forgetChan, args.info.completed)
                        # @async Forgettor(px, args.info.completed)
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
function LogCrawler{Command, Snapshot}(px::Paxos{Command, Snapshot})
    while true
        changed = false
        while true
            next = safe_getSlot(px, px.completed[px.self]+1)
            if isnull(next)
                yield()
                break
            end
            next = next.value
            timeout = () -> @async proposer(px, Command(), next.num) # TODO: Can we guarantee that command has a No_Op more idomatically?
            waitSignal(next.condition, 0.01, 3.0, timeout) # level trigger wait until the slot is done.
            changed = holding(next.lock, "Log Crawler Read $(next.num)") do
                if next.fate != Decided # Should always be true!
                    throw(ErrorException("LogCrawler condtion variable triggered while fate was not decided"))
                end
                px.completed[px.self] += 1
                if !isnull(next.v_a)
                    px.context.transition(px.snapshot, next.v_a.value)
                    true
                end
            end
            if changed
                holding(px.lock, "Log Crawler write") do
                    open(px.context.slot_num_f, "w") do io
                        serialize(io, px.completed[px.self])
                    end
                    open(px.context.snapshot_f, "w") do io
                        @show px.snapshot
                        serialize(io, px.snapshot)
                    end
                end
            end

        end
        wait(Timer(1))
    end
end
function Forgettor{Command, Snapshot}(px::Paxos{Command, Snapshot})
    while true
        completed = take!(px.forgetChan)
        # print("enter Forgettor\n")
        # @show px.completed
        # @show completed
        holding(px.lock, "Forgettor") do
            px.completed = max(px.completed, completed)
            px.forgot = min(px.completed...)
        end
        # @show px.completed

        # Update the forgot index first because we want to prevent the
        # case where we delete a file, then crash, and think our
        # forgot index is too high
        open(px.context.forget_f, "w") do io
            serialize(io, px.forgot)
        end
        for f=readdir(px.context.slot_dir)
            if parse(Int64, f) <= px.forgot
                rm("$(px.context.slot_dir)/$f")
            end
        end

    end
end

function node{Command, Snapshot}(context::Context{Command, Snapshot}, self::Int64, snapshot::Snapshot)
    # print("Starting on $(context.port)\n")
    
    mkpath(context.slot_dir)
    log = Dict{SlotNum, Slot{context.t}}()
    m = -1
    for f=readdir(context.slot_dir)
        open("$(context.slot_dir)/$f") do io
            s = deserialize(io)
            log[s.num] = s
            m = max(m,s.num)
        end
    end

    if isfile(context.snapshot_f)
        open(context.snapshot_f) do io
            snapshot = deserialize(io)
        end
    end

    slot_nums = fill(-1,length(context.peers))
    if isfile(context.slot_num_f)
        open(context.slot_num_f) do io
            slot_nums[self] = deserialize(io)
        end
    end

    forget = -1

    if isfile(context.forget_f)
        open(context.forget_f) do io
            forget = deserialize(io)
        end
    end
    px = Paxos{context.t, context.s}(self, Log{context.t}(log, m),  slot_nums, snapshot, forget,  context)
    @async RPCServer(px)
    @async Forgettor(px)
    LogCrawler(px)
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
