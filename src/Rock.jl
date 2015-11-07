module Rock
using Locks

############################
##    Base/Abstract types ##
############################
typealias ServerId Int64
typealias  LogIndex Int64
typealias Term Int64
typealias Prepare Int64
typealias SlotNum Int
typealias Hostname AbstractString
typealias Port Int64

# Our abstract notion of Paxos
abstract Paxos


# The following API must be supported!!
# TODO: Implement this requirement via traits
#    # function port(px::Paxos)#::Int64
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function peers(px::Paxos)#::Array{Tuple{Hostname, Port},1}
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # end
#    # function transition(px::Paxos, v::ExternalCriticalCommand)#:: Function
#    #     throw(ErrorException("Please Implement"))
#    
#    # end
#    # function syncCompleted(px::Paxos)
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function syncForgot(px::Paxos)
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function syncSnapshot(px::Paxos)
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function syncSlot(px::Paxos, s::Slot)
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function self(px::Paxos)#::Int64
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function log(px::Paxos)#::Log
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function completed(px::Paxos)#::Array{Int64, 1}
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function updateCompleted(px::Paxos, completed::Array{Int64, 1})
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function forgot(px::Paxos)##::Int64
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function context(px::Paxos)#::Context
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function lock(px::Paxos)#::Lock
#    #     throw(ErrorException("Please Implement"))
#    # end
#    # function forgetChan(px::Paxos)#::Channel{Array{Int64, 1}}
#    #     throw(ErrorException("Please Implement"))
#    # end

# Base Message Type, not too useful
abstract BaseMessage
# Command type; cause some action to happen on a server
abstract Command <: BaseMessage
# Resonse as a result of causing a command to happen
abstract Response <: BaseMessage
# Commands that do not need replication
abstract NonCriticalCommand <: Command
# Critical commands need fault tolerance, not instance specific
abstract CriticalCommand <: Command
# Internal critical commands for actions such as membership changes
abstract InternalCriticalCommand <: CriticalCommand
# External critical commands for the user subclass
abstract ExternalCriticalCommand <: CriticalCommand
# Non Critical for consistency, but critical for progress/consensus forming messages.
abstract ConsensusCommand <: Command
abstract ConsensusResponse <: Response
#####################################
##    Components of the log        ##
#####################################
@enum Fate Decided Pending Forgotten
type Slot
    num::SlotNum
    n_p::Int64
    n_a::Int64
    v_a::Nullable{CriticalCommand}
    fate::Fate
    lock::Lock
    condition::LevelTrigger
    # Slot(num, n_p, n_a, v_a, fate) = new(num, n_p, n_a, v_a, fate, Lock(), LevelTrigger())
end
type Log
    slots::Dict{SlotNum, Slot}#Faster structures?
    maxSlot::Int
end
####################################################################################

type PrepareArgs <: ConsensusCommand
    slot_num::SlotNum
    n::Int64
end

type PrepareReply <: ConsensusResponse
    slot_num::SlotNum
    Ok::Bool
    n::Int64
    n_a::Int64
    v_a::Nullable{CriticalCommand}
end

function Handle(px::Paxos,args::PrepareArgs)
    s = safe_getSlot!(px,args.slot_num)
    holding(s.lock, "HandlePrepare $(s.num)") do
        if args.n > s.n_p
            persistently(px, s) do
                s.n_p = args.n
            end
            WithExtraInfo(PrepareReply(args.slot_num, true, s.n_p, s.n_a, s.v_a), ExtraInfo(px))
        else
            WithExtraInfo(PrepareReply(args.slot_num, false, s.n_p, s.n_a, s.v_a), ExtraInfo(px))
        end
    end

end
type AcceptArgs <: ConsensusCommand
    slot_num::SlotNum
    n::Int64
    v::CriticalCommand
end
type AcceptReply <: ConsensusResponse
    slot_num::SlotNum
    Ok::Bool
    n::Int64
    v::CriticalCommand
end
function Handle(px::Paxos, args::AcceptArgs)
    s = safe_getSlot!(px, args.slot_num)
    holding(s.lock, "HandleAccept $(s.num)") do
        if args.n >= s.n_p
            persistently(px,s) do
                s.n_p = args.n
                s.n_a = args.n
                s.v_a = Nullable(args.v)
            end
            WithExtraInfo(AcceptReply(args.slot_num, true, s.n_p, args.v), ExtraInfo(px))
        else
            WithExtraInfo(AcceptReply(args.slot_num, false, 0, NoOp()), ExtraInfo(px))
        end

    end
end
type DecideArgs <: ConsensusCommand
    slot_num::SlotNum
    v::CriticalCommand
end
type DecideReply <: ConsensusResponse
    slot_num::SlotNum
    Ok::Bool
end

function Handle(px::Paxos, args::DecideArgs)
    s = safe_getSlot!(px, args.slot_num)
    holding(s.lock, "$(s.num)") do
        if s.fate != Decided
            persistently(px, s) do
                s.v_a = Nullable(args.v)
                s.fate = Decided
            end
            @async signal(s.condition)
        end
        WithExtraInfo(DecideReply(args.slot_num, true), ExtraInfo(px))
    end
end
#########################
## Other msg Types     ##
#########################
immutable Ping <: NonCriticalCommand
end
immutable PingReply <: Response
end
immutable SwapServer <: InternalCriticalCommand
    old::Tuple{Hostname, Port}
    new::Tuple{Hostname, Port}
end
immutable NoOp <: InternalCriticalCommand
end
function Handle(px::Paxos,args::NoOp)
end
##########################
## PiggyBacking Command ##
##########################

immutable ExtraInfo
    completed::Array{Int64, 1}
    ExtraInfo(px::Paxos) = new(completed(px))
end

function safe_getExtraInfo(px::Paxos)
    holding(lock(px), "getExtraInfo") do
        ExtraInfo(px)
    end
end
type WithExtraInfo
    command
    info::ExtraInfo
end
@inline function getExtraInfo(v::WithExtraInfo)
    v.info
end

function Handle(px::Paxos, command::WithExtraInfo)
    try
        @async put!(forgetChan(px), command.info.completed)
    catch err
        # print(err)
    end
    Handle(px, command.command)
end





##############################################

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

function message(px, m, chan, i)
    host, port = peers(px)[i]
    io = connect(host, port)
    serialize(io, m)
    flush(io)
    val = deserialize(io)
    close(io)
    v =  if typeof(val) == WithExtraInfo
        @async put!(forgetChan(px), val.info.completed)
        val.command
    else
        val
    end
    notNull(chan) do ch
        put!(ch, v)
    end
    v
end
function broadcast(px::Paxos, m, chan)
    @async for i = 1:length(peers(px))
        message(px, m, Nullable(chan), i)
    end
end
function broadcast(px::Paxos, m)
    @async for i = 1:length(peers(px))
        message(px, m, Nullable(),i)
    end
end


function getMaxSlot(l::Log)
    l.maxSlot
end
function getSlot!(px::Paxos, s::SlotNum)
    if s <= forgot(px)
        Slot(0,0,0,Nullable(), Forgotten, Lock(), LevelTrigger())
    else
        l = log(px)
        try
            l.slots[s]
        catch KeyError
            l.slots[s] = Slot(s, 0,0,Nullable(), Pending, Lock(), LevelTrigger())
            l.maxSlot = max(l.maxSlot, s)
            getSlot!(px, s)
        end
    end
end
function safe_getSlot!(px::Paxos, s::SlotNum)
    holding(lock(px), "safe_getSlot! $(s)") do
        getSlot!(px, s)
    end
end

function getSlot(px::Paxos, s::SlotNum)
    if s <= forgot(px)
        Nullable(Slot(0,0,0,Nullable(), Forgotten, Lock(), LevelTrigger()))
    else
        l = log(px)
        try
            Nullable(l.slots[s])
        catch KeyError
            Nullable()
        end
    end
end
function safe_getSlot(px::Paxos, s::SlotNum)
    holding(lock(px), "safe_getSlot $(s)") do
        getSlot(px, s)
    end
end

function persistently(f::Function,  px::Paxos, slot::Slot)
        f()
        syncSlot(px, slot)
end


##################################
##    Proposer functionality    ##
##################################
function acceptPhase(px::Paxos,slot_num::SlotNum, n::Int64, v::CriticalCommand, majority)
    accept = WithExtraInfo(AcceptArgs(slot_num, n, v),  safe_getExtraInfo(px))
    chan = Base.Channel{AcceptReply}(5)
    broadcast(px, accept, chan)
    positives = 0
    negatives = 0
    while true
        r = take!(chan)
        
        if r.Ok
            positives += 1
            if positives >= majority
                decide = WithExtraInfo(DecideArgs(slot_num, v), safe_getExtraInfo(px))
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

function proposer(px::Paxos, v::CriticalCommand, slot_num::SlotNum)
    epoch = -1
    majority = div(length(peers(px)) +2, 2)
    while (holding(lock(px), "Proposer While Loop $slot_num") do
           getSlot!(px, slot_num).fate == Pending
           end)
        epoch += 1
        n = length(peers(px))*epoch + self(px) + 1
        chan = Base.Channel{PrepareReply}(5)# Magic number, not super important! Could be 1
        prep = WithExtraInfo(PrepareArgs(slot_num, n), safe_getExtraInfo(px))
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
function Handle(px::Paxos, args::CriticalCommand)
    slot_num =  holding(lock(px), "Handle Command") do
        getMaxSlot(log(px))+1
    end
    @async proposer(px, args, slot_num)
    slot_num
    
end

function Handle(px::Paxos, args::Ping)
    PingReply()
end



######################
##     Tasklets     ##
######################

function RPCServer(px::Paxos)
    server = listen(port(px))
    while true
        conn = accept(server)
        @async begin
            try
                while true
                    args = deserialize(conn)
                    try
                        r = Handle(px, args)
                        serialize(conn,r)
                    catch err
                        print("Could not handle! $err")
                    end
                end
            catch err
                # print("Connection Failed With $err on node $(self(px))")

            end
        end
    end
end
function LogCrawler(px::Paxos)
    while true
        changed = false
        while true
            next = safe_getSlot(px, completed(px)[self(px)]+1)
            if isnull(next)
                break
            end
            next = next.value
            timeout = () -> @async proposer(px, NoOp(), next.num) # TODO: Can we guarantee that command has a No_Op more idomatically?
            waitSignal(next.condition, 0.01, 3.0, timeout) # level trigger wait until the slot is done.
            changed = holding(next.lock, "Log Crawler Read $(next.num)") do
                if next.fate != Decided # Should always be Decided!
                    throw(ErrorException("LogCrawler condtion variable triggered while fate was not decided"))
                end
                completed(px)[self(px)] += 1
                notNull(next.v_a, false) do v
                    if typeof(v) <: ExternalCriticalCommand
                        transition(px, v)
                        true
                    elseif typeof(next.v_a.value) <: InternalCriticalCommand
                        false
                    else
                        false
                    end
                end
            end
            if changed
                holding(lock(px), "Log Crawler write") do
                    syncCompleted(px)
                    syncSnapshot(px)
                end
            end

        end
        wait(Timer(1))
    end
end
function Forgettor(px::Paxos)
    while true
        completed = take!(forgetChan(px))
        holding(lock(px), "Forgettor") do
            updateCompleted(px, completed) # updates completed and forgotten fields and deletes elements from log
        end

        syncForgot(px)

    end
end

function Heartbeat(px::Paxos)
    while true
        arr = holding(lock(px)) do
            arr = fill(true, length(peers(px)))
            m = Ping()
            @async for i = 1:length(peers(px))
                host, port = peers(px)[i]
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

####################################
##   Paxos Instance Components    ##
##   Not "integrated" to abstract ## 
##   FS out                       ##
####################################
type Context
    port::Int64
    peers::Array{Tuple{Hostname, Port},1}
    transition::Function
    slot_dir
    forget_f
    snapshot_f
    slot_num_f
end
type Instance{ Snapshot} <: Paxos
    self::Int64
    log::Log
    completed::Array{Int64, 1}
    snapshot::Snapshot
    forgot::Int64
    context::Context
    lock::Lock
    forgetChan::Channel{Array{Int64, 1}}
    Instance(self, log, completed, snapshot, forgot, context) = new(self,
                                                                 log, completed, snapshot, forgot, context, Lock(),
                                                                 Channel{Array{Int64, 1}}(5))
end

function port(px::Instance)#::Int64
    px.context.port
end
function peers(px::Instance)#::Array{Tuple{Hostname, Port},1}
    px.context.peers
end
function transition(px::Instance, v::ExternalCriticalCommand)#:: Function
    px.context.transition(px, v)
end
function self(px::Instance)#::Int64
    px.self
end
function log(px::Instance)#::Log
    px.log
end
function completed(px::Instance)#::Array{Int64, 1}
    px.completed
end
function forgot(px::Instance)##::Int64
    px.forgot
end
function context(px::Instance)#::Context
    px.context
end
function lock(px::Instance)#::Lock
    px.lock
end
function forgetChan(px::Instance)#::Channel{Array{Int64, 1}}
    px.forgetChan
end
function updateCompleted(px::Instance, completed)
    px.completed = max(px.completed, completed)
    forgot = min(px.completed...)
    for i = px.forgot:forgot
        delete!(px.log.slots, i)
    end
    px.forgot = forgot
end
function syncSlot(px::Instance, slot::Slot)
    open("$(px.context.slot_dir)/$(slot.num)", "w+") do io
        serialize(io, slot)
    end
end

function syncForgot(px::Instance)
    # Update the forgot index first because we want to prevent the
    # case where we delete a file, then crash, and think our
    # forgot index is too high
    open(context(px).forget_f, "w") do io
        serialize(io, forgot(px))
    end
    for f=readdir(context(px).slot_dir)
        if parse(Int64, f) <= forgot(px)
            rm("$(context(px).slot_dir)/$f")
        end
    end
end
function syncCompleted(px::Instance,)
    open(context(px).slot_num_f, "w") do io
        serialize(io, completed(px)[self(px)])
    end
end

function syncSnapshot(px::Instance)
    open(context(px).snapshot_f, "w") do io
        @show px.snapshot
        serialize(io,px.snapshot)
    end
end
function read_file_or_default(f, d)
    if isfile(f)
        open(f) do io
            deserialize(io)
        end
    else
        d
    end
end
function node{Snapshot}(context::Context, self::Int64, snapshot::Snapshot)
    # print("Starting on $(context.port)\n")

    # Ensure Path exists
    mkpath(context.slot_dir)
    log = Dict{SlotNum, Slot}() 
    m = -1
    for f=readdir(context.slot_dir)
        s= read_file_or_default("$(context.slot_dir)/$f", ()) # () should never be returned
        log[s.num] = s
        m = max(m,s.num)
    end
    snapshot = read_file_or_default(context.snapshot_f, snapshot)

    slot_nums = fill(-1,length(context.peers))
    slot_nums[self] = read_file_or_default(context.slot_num_f, -1)

    forget = read_file_or_default( context.forget_f, -1)

    px = Instance{Snapshot}(self, Log(log, m),  slot_nums, snapshot, forget,  context)
    @async Forgettor(px)
    @async Heartbeat(px)
    @async LogCrawler(px)
    RPCServer(px)

end
type Client
    conn
end

function client(host::AbstractString, port::Int64)
    Client(connect(host, port))
end
function command(c::Client, cmd::Command)

    serialize(c.conn, cmd)
    flush(c.conn)
    deserialize(c.conn)
    
end


end
