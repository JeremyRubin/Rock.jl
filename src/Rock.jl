module Rock
using Locks
typealias ServerId Int64
typealias  LogIndex Int64
typealias Term Int64
typealias Prepare Int64
typealias SlotNum Int
typealias Hostname AbstractString
typealias Port Int64
@enum Fate Decided Pending Forgotten

type ExtraInfo
    completed::Array{Int64, 1}
end
abstract BaseMessage
abstract Command <: BaseMessage
abstract Response <: BaseMessage
abstract NonCriticalCommand <: Command
abstract ConsensusCommand <: Command
abstract InternalConsensusCommand <: ConsensusCommand
abstract ExternalConsensusCommand <: ConsensusCommand

immutable Ping <: NonCriticalCommand
end
immutable PingReply <: Response
end
immutable SwapServer <: InternalConsensusCommand
    old::Tuple{Hostname, Port}
    new::Tuple{Hostname, Port}
end
immutable NoOp <: InternalConsensusCommand
end
####################################################################################
type Slot
    num::SlotNum
    n_p::Int64
    n_a::Int64
    v_a::Nullable{ConsensusCommand}
    fate::Fate
    lock::Lock
    condition::LevelTrigger
    # Slot(num, n_p, n_a, v_a, fate) = new(num, n_p, n_a, v_a, fate, Lock(), LevelTrigger())
end
type PrepareArgs
    slot_num::SlotNum
    n::Int64
    info::ExtraInfo
end
type PrepareReply
    slot_num::SlotNum
    Ok::Bool
    n::Int64
    n_a::Int64
    v_a::Nullable{ConsensusCommand}
    info::ExtraInfo
end
type AcceptArgs
    slot_num::SlotNum
    n::Int64
    v::ConsensusCommand
    info::ExtraInfo
end
type AcceptReply
    slot_num::SlotNum
    Ok::Bool
    n::Int64
    v::ConsensusCommand
    info::ExtraInfo
end
type DecideArgs
    slot_num::SlotNum
    v::ConsensusCommand
    info::ExtraInfo
end
type DecideReply
    slot_num::SlotNum
    Ok::Bool
    info::ExtraInfo
end



type Log
    slots::Dict{SlotNum, Slot}
    maxSlot::Int
end



type Context
    port::Int64
    peers::Array{Tuple{Hostname, Port},1}
    transition::Function
    slot_dir
    forget_f
    snapshot_f
    slot_num_f
end
type Paxos{ Snapshot}
    self::Int64
    log::Log
    completed::Array{Int64, 1}
    snapshot::Snapshot
    forgot::Int64
    context::Context
    lock::Lock
    forgetChan::Channel{Array{Int64, 1}}
    Paxos(self, log, completed, snapshot, forgot, context) = new(self,
    log, completed, snapshot, forgot, context, Lock(),
    Channel{Array{Int64, 1}}(5))
end

@inline function notNull(f::Function, m_v, def)
    if !isnull(m_v)
        f(m_v.value)
    else
        def
    end

end
@inline function notNull(f::Function, m_v)
    if !isnull(m_v)
        f(m_v.value)
    end
end

function getExtraInfo{S}(px::Paxos{S})
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
    notNull(chan) do ch
        put!(ch, val)
    end
    try
        put!(px.forgetChan, val.info.completed)
    catch err
        print(err)
    end
    val
end
function broadcast{S}(px::Paxos{S}, m, chan)
    @async for i = 1:length(px.context.peers)
        message(px, m, Nullable(chan), i)
    end
end
function broadcast{S}(px::Paxos{S}, m)
    @async for i = 1:length(px.context.peers)
        message(px, m, Nullable(),i)
    end
end


function getMaxSlot(l::Log)
    l.maxSlot
end
function getSlot!{S}(px::Paxos{S}, s::SlotNum)
    if s <= px.forgot
        Slot(0,0,0,Nullable(), Forgotten, Lock(), LevelTrigger())
    else
        l = px.log    
        try
            l.slots[s]
        catch KeyError
            l.slots[s] = Slot(s, 0,0,Nullable(), Pending, Lock(), LevelTrigger())
            l.maxSlot = max(l.maxSlot, s)
            getSlot!(px, s)
        end
    end
end
function safe_getSlot!{S}(px::Paxos{S}, s::SlotNum)
    holding(px.lock, "safe_getSlot! $(s)") do
        getSlot!(px, s)
    end
end

function getSlot{S}(px::Paxos{S}, s::SlotNum)
    if s <= px.forgot
        Nullable(Slot(0,0,0,Nullable(), Forgotten, Lock(), LevelTrigger()))
    else
        l = px.log    
        try
            Nullable(l.slots[s])
        catch KeyError
            Nullable()
        end
    end
end
function safe_getSlot{S}(px::Paxos{S}, s::SlotNum)
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
function Handle{S}(px::Paxos{S},args::PrepareArgs)
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
function Handle{S}(px::Paxos{S},args::NoOp)
end

function Handle{S}(px::Paxos{S}, args::AcceptArgs)
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
            AcceptReply(args.slot_num, false, 0, NoOp(), ExtraInfo( px.completed))
        end

    end
end
function Handle{S}(px::Paxos{S}, args::DecideArgs)
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

function acceptPhase{S}(px::Paxos{S},slot_num::SlotNum, n::Int64, v::ConsensusCommand, majority)
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

function proposer{S}(px::Paxos{S}, v::ConsensusCommand, slot_num::SlotNum)
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
                    notNull(r.v_a) do v_new
                        v_a = v_new
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
function Handle{S}(px::Paxos{S}, args::ConsensusCommand)
    slot_num =  holding(px.lock, "Handle Command") do
        getMaxSlot(px.log)+1
    end
    @async proposer(px, args, slot_num)
    slot_num
    
end

function Handle{ S}(px::Paxos{ S}, args::Ping)
    PingReply()
end


function RPCServer{Snapshot}(px::Paxos{Snapshot})
    server = listen(px.context.port)
    while true
        conn = accept(server)
        @async begin
            try
                while true
                    args = deserialize(conn)
                    @show args
                    try
                        r = Handle(px, args)
                        serialize(conn,r)
                        try
                            put!(px.forgetChan, args.info.completed)
                        catch err
                            # print(err)
                        end
                    catch err
                        print("Could not handle! $err")
                    end
                end
            catch err
                # print("Connection Failed With $err on node $(px.self)")

            end
        end
    end
end
function LogCrawler{ Snapshot}(px::Paxos{Snapshot})
    while true
        changed = false
        while true
            next = safe_getSlot(px, px.completed[px.self]+1)
            if isnull(next)
                break
            end
            next = next.value
            timeout = () -> @async proposer(px, NoOp(), next.num) # TODO: Can we guarantee that command has a No_Op more idomatically?
            waitSignal(next.condition, 0.01, 3.0, timeout) # level trigger wait until the slot is done.
            changed = holding(next.lock, "Log Crawler Read $(next.num)") do
                if next.fate != Decided # Should always be true!
                    throw(ErrorException("LogCrawler condtion variable triggered while fate was not decided"))
                end
                px.completed[px.self] += 1
                notNull(next.v_a, false) do v
                    if typeof(v) <: ExternalConsensusCommand
                        px.context.transition(px.snapshot, v)
                        true
                    elseif typeof(next.v_a.value) <: InternalConsensusCommand
                        false
                    else
                        @show typeof(next.v_a.value)
                        false
                    end
                end
            end
            if changed
                holding(px.lock, "Log Crawler write") do
                    open(px.context.slot_num_f, "w") do io
                        serialize(io, px.completed[px.self])
                    end
                    open(px.context.snapshot_f, "w") do io
                        serialize(io, px.snapshot)
                    end
                end
            end

        end
        wait(Timer(1))
    end
end
function Forgettor{Snapshot}(px::Paxos{Snapshot})
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

function Heartbeat{Snapshot}(px::Paxos{Snapshot})
    while true
        arr = holding(px.lock) do
            arr = fill(true, length(px.context.peers))
            m = Ping()
            @async for i = 1:length(px.context.peers)
                host, port = px.context.peers[i]
                try
                    io = connect(host, port)
                    serialize(io, m)
                    flush(io)
                    deserialize(io)
                    close(io)
                    arr[i] = false
                catch err
                    # Wait, then Retry!
                    wait(Timer(5))
                    try
                        io = connect(host, port)
                        serialize(io, m)
                        flush(io)
                        deserialize(io)
                        close(io)
                        arr[i] = false
                    catch err
                        #@show err
                    end
                end
            end
            arr
        end
        wait(Timer(10))
        for (i,v) = enumerate(arr)
            if v
                print("$i dead!\n")
            end
        end
            
    end
end

function node{Snapshot}(context::Context, self::Int64, snapshot::Snapshot)
    # print("Starting on $(context.port)\n")
    
    mkpath(context.slot_dir)
    log = Dict{SlotNum, Slot}()
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
    px = Paxos{Snapshot}(self, Log(log, m),  slot_nums, snapshot, forget,  context)
    @async RPCServer(px)
    @async Forgettor(px)
    @async Heartbeat(px)
    LogCrawler(px)

end
type Client
    conn
end

function client(host::AbstractString, port::Int64)
    Client(connect(host, port))
end
function command{Command}(c::Client, cmd::Command)

    serialize(c.conn, cmd)
    flush(c.conn)
    deserialize(c.conn)
    
end


end
