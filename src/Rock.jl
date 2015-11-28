module Rock
using LockUtils

using SQLite
using Logging
@Logging.configure(level=INFO)
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
#    # function peers(px::Paxos)#::Array{Tuple{Hostname, Port},1}
#    # function user_defined_function(px::Paxos, v::ExternalCriticalCommand)#:: Function
#    # function syncCompleted(px::Paxos)
#    # function syncForgot(px::Paxos)
#    # function syncSlot(px::Paxos, s::Slot)
#    # function self(px::Paxos)#::Int64
#    # function completed(px::Paxos)#::Array{Int64, 1}
#    # function updateCompleted(px::Paxos, completed::Array{Int64, 1})
#    # function forgot(px::Paxos)##::Int64
#    # function context(px::Paxos)#::Context
#    # function lock(px::Paxos)#::Lock

# Base Message Type, not too useful
abstract BaseMessage
# Command type; cause some action to happen on a server
abstract Command <: BaseMessage
# Resonse as a result of causing a command to happen
abstract Response <: BaseMessage
# Commands that do not need replication
abstract NonCriticalCommand <: Command
# Commands that take a user defined function and run them, without consensus
abstract ExternalNonCriticalCommand <: NonCriticalCommand
abstract InternalNonCriticalCommand <: NonCriticalCommand
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

function blockingTransaction(f::Function, px::Paxos, d::SQLite.DB)
    # while true
    #     try
    #         @debug "Trying transaction"
    Base.lock(px.dblock)
    v = SQLite.transaction(d) do
        f()
    end
    Base.unlock(px.dblock)
    return v
    #     catch err
    #         if isa(err, SQLite.SQLiteException) && err.msg == "database is locked"
    #             @debug "Transaction Retrying"
    #             wait(Timer(0.01))
    #             continue
    #         else
    #             rethrow()
    #         end
    #     end
    # end
end
function Handle(px::Paxos, args::PrepareArgs)
    @debug "PREPARE"
    d = db(px)
    blockingTransaction(px, d) do
        s = unsafe_getSlot!(px,d,args.slot_num)
        if args.n > s.n_p
            persistently(d, s) do
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
    d = db(px)
    blockingTransaction(px,d) do
        s = unsafe_getSlot!(px,d, args.slot_num)
        if args.n >= s.n_p
            persistently(d,s) do
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
    d = db(px)
    blockingTransaction(px,d) do
        s = unsafe_getSlot!(px,d, args.slot_num)
        if s.fate != Decided
            persistently(d, s) do
                s.v_a = Nullable(args.v)
                s.fate = Decided
            end
            # @async signal(s.condition)
        end
        WithExtraInfo(DecideReply(args.slot_num, true), ExtraInfo(px))
    end
end
#########################
## Other msg Types     ##
#########################
type Ping <: InternalNonCriticalCommand
    msg::ASCIIString
end
function Handle(px::Paxos, args::Ping)
    @debug args.msg
    PingReply()
end
type PingReply <: Response
end
function Handle(px::Paxos, args::PingReply)
    @warn "THIS SHOULD NOT HAPPEN EVER"
end

# TODO
type DeadLeader <: InternalCriticalCommand
    id::Int64
end

type AddPeer <: InternalCriticalCommand
    new::Tuple{Hostname, Port}
end
type DeletePeer <: InternalCriticalCommand
    dead_peer::Tuple{Hostname, Port}
end
type SwapPeer <: InternalCriticalCommand
    old::DeletePeer
    new::AddPeer
end
type NoOp <: InternalCriticalCommand
end
function Handle(px::Paxos,args::NoOp)
end
##########################
## PiggyBacking Command ##
##########################

type ExtraInfo
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
        @async Forgettor(px, command.info.completed)
    catch err
        @debug err
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
    conn = connect(host, port)
    serialize(conn, m)
    val = deserialize(conn)
    close(conn)
    v =  if isa(val,WithExtraInfo)
        @async Forgettor(px, val.info.completed)
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

function unsafe_getSlot!(px::Paxos,d::SQLite.DB, s::SlotNum)
    @debug "unsafe_getSlot! $(forgot(px)) $s"
    if s <= forgot(px)
        Slot(0,0,0,Nullable(), Forgotten)
    else
        obj = SQLite.query(d, "select slot, n_p,n_a, v_a, fate from slots where (slot=?) limit 1", [s]).data
        @debug obj
        if length(obj[1]) > 0 
            @debug "GOT OBJ"
            Slot(obj[1][1].value, obj[2][1].value,obj[3][1].value, obj[4][1].value, obj[5][1].value)
        else
            v = Slot(s, 0,0,Nullable(), Pending)
            @debug "CEHCK"
            SQLite.query(d, "insert into slots values (?, ?, ?, ?, ?)", [s, 0, 0,  Pending, Nullable{CriticalCommand}()])
            v
        end
    end
end

function getSlot!(px::Paxos, s::SlotNum)
    d = db(px)
    blockingTransaction(px,d) do
        unsafe_getSlot!(px,d, s)
    end
end
function safe_getSlot!(px::Paxos, s::SlotNum)
    holding(lock(px), "safe_getSlot! $(s)") do
        getSlot!(px, s)
    end
end

function getSlot(px::Paxos, s::SlotNum)
    d = db(px)
    if s <= forgot(px)
        Nullable(Slot(0,0,0,Nullable(), Forgotten))
    else
        obj = SQLite.query(d, "select slot, n_p,n_a, v_a, fate from slots where (slot=?) limit 1", [s]).data
        if length(obj[1]) > 0
            Nullable(Slot(obj[1][1].value, obj[2][1].value,obj[3][1].value, obj[4][1].value,(obj[5][1].value)))
        else
            Nullable()
        end
    end

end
function safe_getSlot(px::Paxos, s::SlotNum)
    holding(lock(px), "safe_getSlot $(s)") do
        getSlot(px, s)
    end
end

function persistently(f::Function,  d::SQLite.DB, slot::Slot)
    f()
    syncSlot(d, slot)
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
           @debug "GETTING FATE"
           getSlot!(px, slot_num).fate == Pending
           end)
        epoch += 1
        n = length(peers(px))*epoch + self(px) + 1
        chan = Base.Channel{PrepareReply}(5)# Magic number, not super important! Could be 1
        prep = WithExtraInfo(PrepareArgs(slot_num, n), safe_getExtraInfo(px))
        @info "Proposing on slot $slot_num on $(self(px)) with value $v"
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
    d = db(px)
    holding(lock(px), "Handle Command") do
        blockingTransaction(px,d)  do
            SQLite.query(d, "insert into slots values (null, ?, ?, ?, ?)", [0, 0,  Pending,Nullable{CriticalCommand}()])
            # Only safe because on one connection
            s = SQLite.query(d, "select last_insert_rowid()").data[1][1].value
            @async proposer(px, args, s)
            s
        end
    end
    
end
function Handle(px::Paxos, args::ExternalNonCriticalCommand)
    user_defined_function(px,db(px), args)
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
            while true
                try
                    args = deserialize(conn)
                    @debug "RPCServer Args: $args"
                    try
                        r = Handle(px, args)
                        @debug "RPCServer Response: $r"
                        serialize(conn,r)
                        flush(conn)
                    catch err
                        @debug "Could not handle!\n$args\n $err"
                        close(conn)
                        break
                    end
                catch err
                    @debug "Connection Failed With $err on node $(self(px))"
                    close(conn)
                    break
                # finally
                #     close(conn)
                end
            end
        end
    end
end
function LogCrawler(px::Paxos)
    @debug "Log Crawler Starting"
    while true
        changed = false
        while true
            next = safe_getSlot(px, completed(px)[self(px)]+1)
            if isnull(next)
                wait(Timer(0.01))
                break
            end
            next = next.value
            times = Int64(floor((rand()*10+5.0)/0.01)) # Wait at least 5 seconds worth, but randomize
            count = 0
            fired = false
            while true
                if count > times && !fired
                    #Timeout
                    fired = true
                    @warn "Timeout in Log Crawler, starting Proposer"
                    @async proposer(px, NoOp(), next.num) # TODO: Can we guarantee that command has a No_Op more idomatically?
                    wait(Timer(0.01))
                end
                count += 1
                d = db(px)
                finished  = blockingTransaction(px,d) do
                    result = SQLite.query(d, "select v_a from slots where fate = ? and slot = ? limit 1", [Decided, next.num]).data
                    if length(result[1])> 0 && !isnull(result[1][1])
                        command = result[1][1].value.value # must not be null, otherwise consensus broken somewhere else
                        completed(px)[self(px)] += 1
                        syncCompleted(px,d)
                        @info "LogCrawler completed Command $command on $(self(px))"
                        if typeof(command) <: ExternalCriticalCommand
                            @debug "running user_defined_function function"
                            user_defined_function(px,d, command)
                        elseif typeof(command) <: InternalCriticalCommand
                        else
                        end
                        true
                    else
                        false
                    end
                end
                # @debug "Completed a log entry? $finished"
                if finished
                    break
                else
                    wait(Timer(0.01))
                end
            end
        end

        

    end
    wait(Timer(1))
end
function Forgettor(px::Paxos, completed::Array{Int64,1})
    @debug "Forgetting up to $completed"
    holding(lock(px), "Forgettor") do
        updateCompleted(px, completed) # updates completed and forgotten fields and deletes elements from log
        d = db(px)
        syncForgot(px,d)

    end
end

function Heartbeat(px::Paxos)
    while true
        @debug "Begin Heartbeat"
        arr = holding(lock(px)) do
            arr = fill(true, length(peers(px)))
            m = Ping("TEST")
            @async for i = 1:length(peers(px))
                host, port = peers(px)[i]
                try
                    conn = connect(host, port)
                    try
                        serialize(conn, m)
                        deserialize(conn)
                        # arr[i] = false
                    catch err
                        # Wait, then Retry!
                        wait(Timer(5))
                        conn2 = connect(host, port)
                        try
                            serialize(conn2, m)
                            flush(conn2)
                            deserialize(conn2)
                            # arr[i] = false
                        catch err
                            @debug err
                        finally
                            close(conn2)
                        end
                    finally
                        close(conn)
                    end
                catch err
                    @debug err
                end
            end
            arr
        end
        wait(Timer(10))
        for (i,v) = enumerate(arr)
            if v
                @warn "$i dead"
            end
        end
        @debug "End Heartbeat"
        
    end
end

####################################
##   Paxos Instance Components    ##
##   Not "integrated" to abstract ## 
##   FS out                       ##
####################################
type Context
    port::Port
    peers::Array{Tuple{ASCIIString, Int64}, 1}
    user_defined_function::Function
    db_loc::AbstractString
    function Context(port::Port, peers::Array{Tuple{ASCIIString, Int64},1}, user_defined_function::Function, db_loc::ASCIIString)
        d = SQLite.DB(db_loc)
        SQLite.transaction(d) do
            try
                SQLite.query(d, "create table parameters (K TEXT Unique PRIMARY KEY, V BLOB)")
            catch SQLite.SQLiteException
            end
            try
                SQLite.query(d, "create table slots (slot INTEGER PRIMARY KEY AUTOINCREMENT, n_p INT64, n_a INT64, fate INT, v_a BLOB)")
            catch SQLite.SQLiteException
            end
            try
                SQLite.query(d, "create table peers (host TEXT, port Int, unique (host, port))")
                for (h, p)= peers
                    SQLite.query(d, "insert or ignore into peers values (?, ?)", [h, p])
                end
            catch SQLite.SQLiteException
            end

        end
         new(port, peers, user_defined_function, db_loc)
        
    end
end
function db(context::Context)
    SQLite.DB(context.db_loc)
end
type Instance <: Paxos
    self::Int64
    completed::Array{Int64, 1}
    forgot::Int64
    context::Context
    lock::Lock
    dblock::ReentrantLock
    function Instance(context::Context, self::Int64)

        d  = db(context)
        # Ensure Path exists
        # m = -1
        # data = SQLite.query(d, "select max(slot) from slots").data[1]
        # if length(data) >0
        #     notNull(data[1]) do v
        #         m = max(m,v)
        #     end
        # end

        v = SQLite.query(d, "select V from parameters where (K = 'forgot')").data[1]
        forgot = 0
        if length(v) > 0 && !isnull(v[1])
            forgot = v[1].value 
        end
        completed = fill(forgot,length(context.peers))
        v = SQLite.query(d, "select V from parameters where (K = 'completed')").data[1]
        if length(v) > 0 && !isnull(v[1])
            completed[self] = v[1].value
        end
        new(self, completed, forgot, context, Lock(), ReentrantLock())
    end
end
function start(px::Instance)
        @info "Starting Node on $(port(px))" 
        @async LogCrawler(px)
        # @async Heartbeat(px)
        RPCServer(px)
end

function port(px::Instance)#::Int64
    px.context.port
end
function peers(px::Instance)#::Array{Tuple{Hostname, Port},1}
    px.context.peers
    # d = db(px)
    # data = SQLite.query(d, "select host, port from peers").data
    # collect(zip(map(v -> v.value, data[1]), map(v -> v.value, data[2])))
end
function addpeer(px::Instance, host, port)#::Array{Tuple{Hostname, Port},1}
    d = db(px)
    data = SQLite.query(d,  "insert or ignore into peers values (?, ?)", [host, port])
end
function deletepeer(px::Instance, host, port)#::Array{Tuple{Hostname, Port},1}
    d = db(px)
    data = SQLite.query(d, "delete from peers where (host = ?, port = ?)", [host, port])
end
function user_defined_function(px::Instance, d::SQLite.DB, v::ExternalCriticalCommand)#:: Function
    px.context.user_defined_function(px, d, v)
end
function user_defined_function(px::Instance, d::SQLite.DB, v::ExternalNonCriticalCommand)#:: Function
    px.context.user_defined_function(px, d, v)
end
function user_defined_function(px::Instance)
    px.context.user_defined_function
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
function db(px::Instance)
    db(context(px))
end
function syncSlot(d::SQLite.DB, slot::Slot)
    SQLite.query(d, "insert or replace into slots values (?, ?, ?, ?, ?)",[slot.num, slot.n_p, slot.n_a, slot.fate, slot.v_a  ]) 
end

function updateCompleted(px::Instance, completed)
    px.completed = max(px.completed, completed) # Note this is vectorized
    px.forgot = min(px.completed...)
end
function syncCompleted(px::Instance, d::SQLite.DB)
    SQLite.query(d, "insert or replace into parameters values ('completed', ?)",[completed(px)[self(px)]]) 
end
function syncForgot(px::Instance, d::SQLite.DB)
    # Update the forgot index first because we want to prevent the
    # case where we delete a file, then crash, and think our
    # forgot index is too high
    blockingTransaction(px,d) do
        SQLite.query(d, "insert or replace into parameters values ('forgot', ?)",[forgot(px)]) 
        SQLite.query(d, "delete from slots where  (slot <= ?)",[forgot(px)]) 
        syncCompleted(px, d)
    end
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
