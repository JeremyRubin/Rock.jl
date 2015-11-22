module Rock
using LockUtils

using SQLite
using Logging
@Logging.configure(level=DEBUG)
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
immutable Slot
    num::SlotNum
    n_p::Int64
    n_a::Int64
    v_a::Nullable{CriticalCommand}
    fate::Fate
end
####################################################################################

immutable PrepareArgs <: ConsensusCommand
    slot_num::SlotNum
    n::Int64
end

immutable PrepareReply <: ConsensusResponse
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
immutable AcceptArgs <: ConsensusCommand
    slot_num::SlotNum
    n::Int64
    v::CriticalCommand
end
immutable AcceptReply <: ConsensusResponse
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
immutable DecideArgs <: ConsensusCommand
    slot_num::SlotNum
    v::CriticalCommand
end
immutable DecideReply <: ConsensusResponse
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


function Handle(px::Paxos, args::Ping)
    PingReply()
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
immutable WithExtraInfo
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


function getMaxSlot(px::Paxos)
    d = SQLite.query(db(px), "select max(K) from slots").data[1]
    if length(d) == 1
        isNull(d, -1) do v
            v
        end
    else
        -1
    end
    
end
function getSlot!(px::Paxos, s::SlotNum)
    if s <= forgot(px)
        Slot(0,0,0,Nullable(), Forgotten)
    else
        SQLite.transaction(db(px)) do
            obj = SQLite.query(db(px), "select K, n_p,n_a, v_a, fate from slots where (K=?) limit 1", [s]).data
            if length(obj[1]) > 0 
                command = deserialize(IOBuffer(obj[4]))
                Slot(obj[1].value, obj[2].value,obj[3].value, command, Fate(obj[5].value))
            else
                v = Slot(s, 0,0,Nullable(), Pending)
                SQLite.query(db(px), "insert into slots values (?, ?, ?, ?, ?)", [s, 0, 0,  Int(Pending), Nullable()])
                v
            end
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
        Nullable(Slot(0,0,0,Nullable(), Forgotten))
    else
        SQLite.transaction(db(px)) do
            obj = SQLite.query(db(px), "select K, n_p,n_a, v_a, fate from slots where (K=?) limit 1", [s]).data
            if length(obj[1]) > 0
                command = deserialize(IOBuffer(obj[4]))
                Nullable(Slot(obj[1].value, obj[2].value,obj[3].value, command, Fate(obj[5].value)))
            else
                Nullable()
            end
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
    holding(lock(px), "Handle Command") do
        s = getMaxSlot(log(px))+1
        @async proposer(px, args, s)
        s
    end
    
end
######################
##     Tasklets     ##
######################

function RPCServer(px::Paxos)
    server = listen(port(px))
    @info "RPC Server Starting"
    while true
        conn = accept(server)
        @debug "Got a Connection"
        @async begin
            try
                while true
                    args = deserialize(conn)
                    @debug args
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
            @show next
            next = next.value
            times = Int64(3.0/0.1)
            count = 0
            while true
                if count > times
                    #Timeout
                    @async proposer(px, NoOp(), next.num) # TODO: Can we guarantee that command has a No_Op more idomatically?
                end
                count += 1
                SQLite.transaction(db(px)) do
                    d = SQLite.query(db(px), "select v_a from slots where Fate = ? and K = ? limit 1", [Decided, next.num]).data
                    if length(d.data[1])> 0 && !isnull(d.data[1][1])
                        command = d.data[1][1].value
                        completed(px)[self(px)] += 1
                        if typeof(command) <: ExternalCriticalCommand
                            SQLite.transaction(db(px)) do
                                transition(px, command)
                                syncCompleted(px)
                                true
                            end
                        elseif typeof(command) <: InternalCriticalCommand
                            false
                        else
                            false
                        end
                    end
                end
            end
            wait(Timer(0.01))
        end

        

    end
    wait(Timer(1))
end
function Forgettor(px::Paxos)
    while true
        completed = take!(forgetChan(px))
        holding(lock(px), "Forgettor") do
            updateCompleted(px, completed) # updates completed and forgotten fields and deletes elements from log
            syncForgot(px)
        end


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
    db::SQLite.DB
    function Context(port, peers, transition, db_loc::AbstractString)
        db = SQLite.DB(db_loc)
        SQLite.transaction(db) do
            try
                SQLite.query(db, "create table parameters (K TEXT Unique PRIMARY KEY, V TEXT)")
            catch SQLite.SQLiteException
            end


            try
                SQLite.query(db, "create table slots (K INTEGER PRIMARY KEY, n_p INT64, n_a INT64, fate INT, v_a BLOB NULLABLE)")
            catch SQLite.SQLiteException
            end
            try
                SQLite.query(db, "create table snapshot (K TEXT PRIMARY KEY, V BLOB)")
            catch SQLite.SQLiteException
            end

        end
        new(port, peers, transition, db)
    end
end
type Instance <: Paxos
    self::Int64
    completed::Array{Int64, 1}
    forgot::Int64
    context::Context
    lock::Lock
    forgetChan::Channel{Array{Int64, 1}}
    Instance(self, completed, forgot, context) = new(self,
                                                          completed, forgot, context, Lock(),
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
function db(px::Instance)
    context(px).db
end
function updateCompleted(px::Instance, completed)
    px.completed = max(px.completed, completed) # Note this is vectorized
    px.forgot = min(px.completed...)
end
function syncSlot(px::Instance, slot::Slot)
    a = IOBuffer()
    serialize(a, slot)
    b = bytestring(a)
    SQLite.query(db(px), "insert or replace into slots values (?, ?)",[slot.num, b]) 
end

function syncForgot(px::Instance)
    # Update the forgot index first because we want to prevent the
    # case where we delete a file, then crash, and think our
    # forgot index is too high
    SQLite.transaction(db(px)) do
        SQLite.query(db(px), "insert or replace into parameters values ('forgot', ?)",[forgot(px)]) 
        SQLite.query(db(px), "delete from slots where  (K <= ?)",[forgot(px)]) 
    end
end
function syncCompleted(px::Instance,)
    SQLite.transaction(db(px)) do
        SQLite.query(db(px), "insert or replace into parameters values ('completed', ?)",[completed(px)[self(px)]]) 
    end
end

function node(context::Context, self::Int64)
    # print("Starting on $(context.port)\n")

    # Ensure Path exists
    log = Dict{SlotNum, Slot}() 
    m = -1
    d = SQLite.query(context.db, "select max(K) from slots").data[1]
    if length(d) >0
        notNull(d[1]) do v
            m = max(m,v)
        end
    end

    slot_nums = fill(-1,length(context.peers))
    v = SQLite.query(context.db, "select * from parameters where (K = 'completed')").data[2]
    if length(v) > 0 && !isnull(v[1])
        slot_nums[self] = v[1].value
    end
    v = SQLite.query(context.db, "select * from parameters where (K = 'forgot')").data[2]
    forgot = -1
    if length(v) > 0 && !isnull(v[1])
        forgot = v[1].value 
    end

    px = Instance(self, slot_nums, forgot,  context)
    println("Starting Node on $(context.port)")
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
