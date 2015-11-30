
module Server
using Rock.RPC
# TODO: Clean this up
import Rock.RPC: BaseMessage, Command , Response , NonCriticalCommand, ExternalNonCriticalCommand, InternalNonCriticalCommand, CriticalCommand , InternalCriticalCommand, ExternalCriticalCommand, ConsensusCommand , ConsensusResponse , Fate, Decided, Pending, Forgotten, Slot, PrepareArgs , PrepareReply , AcceptArgs , AcceptReply , DecideArgs , DecideReply , Ping , PingReply , DeadLeader , AddPeer , DeletePeer , SwapPeer , NoOp , ExtraInfo, WithExtraInfo, SlotNum, Hostname, Port

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

# Our abstract notion of t
####################################
##   t t Components    ##
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
type t
    self::Int64
    completed::Array{Int64, 1}
    forgot::Int64
    context::Context
    lock::Lock
    dblock::ReentrantLock
    rpc_task::Task
    log_task::Task
    function t(context::Context, self::Int64)

        d  = db(context)
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

function start(px::t)
    @info "Starting Node on $(port(px))"
    px.log_task = @async LogCrawler(px)
    px.rpc_task = @async RPCServer(px)
    wait(px.rpc_task)
    # @async Heartbeat(px)
end

function port(px::t)#::Int64
    px.context.port
end
function peers(px::t)#::Array{Tuple{Hostname, Port},1}
    px.context.peers
    # d = db(px)
    # data = SQLite.query(d, "select host, port from peers").data
    # collect(zip(map(v -> v.value, data[1]), map(v -> v.value, data[2])))
end
function addpeer(px::t, host, port)#::Array{Tuple{Hostname, Port},1}
    d = db(px)
    data = SQLite.query(d,  "insert or ignore into peers values (?, ?)", [host, port])
end
function deletepeer(px::t, host, port)#::Array{Tuple{Hostname, Port},1}
    d = db(px)
    data = SQLite.query(d, "delete from peers where (host = ?, port = ?)", [host, port])
end
function user_defined_function(px::t, d::SQLite.DB, v::ExternalCriticalCommand)#:: Function
    px.context.user_defined_function(px, d, v)
end
function user_defined_function(px::t, d::SQLite.DB, v::ExternalNonCriticalCommand)#:: Function
    px.context.user_defined_function(px, d, v)
end
function user_defined_function(px::t)
    px.context.user_defined_function
end
function self(px::t)#::Int64
    px.self
end
function completed(px::t)#::Array{Int64, 1}
    px.completed
end
function forgot(px::t)##::Int64
    px.forgot
end
function context(px::t)#::Context
    px.context
end
function lock(px::t)#::Lock
    px.lock
end
function db(px::t)
    db(context(px))
end
function syncSlot(d::SQLite.DB, slot::Slot)
    SQLite.query(d, "insert or replace into slots values (?, ?, ?, ?, ?)",[slot.num, slot.n_p, slot.n_a, slot.fate, slot.v_a  ])
end

function updateCompleted(px::t, completed)
    px.completed = max(px.completed, completed) # Note this is vectorized
    px.forgot = min(px.completed...)
end
function syncCompleted(px::t, d::SQLite.DB)
    SQLite.query(d, "insert or replace into parameters values ('completed', ?)",[completed(px)[self(px)]])
end
function syncForgot(px::t, d::SQLite.DB)
    # Update the forgot index first because we want to prevent the
    # case where we delete a file, then crash, and think our
    # forgot index is too high
    blockingTransaction(px,d) do
        SQLite.query(d, "insert or replace into parameters values ('forgot', ?)",[forgot(px)])
        SQLite.query(d, "delete from slots where  (slot <= ?)",[forgot(px)])
        syncCompleted(px, d)
    end
end


############################################
#          Handlers                        #
############################################
function Handle(px::t, args::PrepareArgs)
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
function Handle(px::t, args::AcceptArgs)
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

function Handle(px::t, args::DecideArgs)
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
function Handle(px::t, args::Ping)
    @debug args.msg
    PingReply()
end
function Handle(px::t, args::PingReply)
    @warn "THIS SHOULD NOT HAPPEN EVER"
end
function Handle(px::t,args::NoOp)
end
#=
Piggybacking Commands
=#

function ExtraInfo(px::t)
    ExtraInfo(completed(px))
end
function safe_getExtraInfo(px::t)
    holding(lock(px), "getExtraInfo") do
        ExtraInfo(px)
    end
end
@inline function getExtraInfo(v::WithExtraInfo)
    v.info
end

function Handle(px::t, command::WithExtraInfo)
    try
        @async Forgettor(px, command.info.completed)
    catch err
        @error err
    end
    Handle(px, command.command)
end

function Handle(px::t, args::CriticalCommand)
    try
        d = db(px)
        while true
            t = holding(lock(px), "Handle Command") do
                @debug "GOT LOCK 1"

                blockingTransaction(px,d)  do
                @debug "GOT LOCK 2"
                    SQLite.query(d, "insert into slots values (null, ?, ?, ?, ?)", [0, 0,  Pending,Nullable{CriticalCommand}()])
                    # Only safe because on one connection
                    s = SQLite.query(d, "select last_insert_rowid()").data[1][1].value
                    @async Proposer(px, args, s, true)
                end
            end
                @debug "WAITING"
            if consume(t)

                @debug "FINISHED"
                return true
            end
            @debug "RETRYING"
        end
    catch err
        @debug err
        return false
    end

end
function Handle(px::t, args::ExternalNonCriticalCommand)
    user_defined_function(px,db(px), args)
end




##########################################
#         Utility                       ##
##########################################

function blockingTransaction(f::Function, px::t, d::SQLite.DB)
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

function message(px::t, m::BaseMessage, chan, i::Int64)
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
function broadcast(px::t, m::BaseMessage, chan)
    @async for i = 1:length(peers(px))
        message(px, m, Nullable(chan), i)
    end
end
function broadcast(px::t, m::BaseMessage)
    @async for i = 1:length(peers(px))
        message(px, m, Nullable(),i)
    end
end

function unsafe_getSlot!(px::t,d::SQLite.DB, s::SlotNum)
    @debug "unsafe_getSlot! $(forgot(px)) $s"
    if s <= forgot(px)
        Slot(0,0,0,Nullable(), Forgotten)
    else
        obj = SQLite.query(d, "select slot, n_p,n_a, v_a, fate from slots where (slot=?) limit 1", [s]).data
        if length(obj[1]) > 0
            @debug "Got Slot $(Slot(obj[1][1].value, obj[2][1].value,obj[3][1].value, obj[4][1].value, obj[5][1].value))"

            Slot(obj[1][1].value, obj[2][1].value,obj[3][1].value, obj[4][1].value, obj[5][1].value)
        else
            v = Slot(s, 0,0,Nullable(), Pending)
            SQLite.query(d, "insert into slots values (?, ?, ?, ?, ?)", [s, 0, 0,  Pending, Nullable{CriticalCommand}()])
            v
        end
    end
end

function getSlot!(px::t, s::SlotNum)
    d = db(px)
    blockingTransaction(px,d) do
        unsafe_getSlot!(px,d, s)
    end
end
function safe_getSlot!(px::t, s::SlotNum)
    holding(lock(px), "safe_getSlot! $(s)") do
        getSlot!(px, s)
    end
end

function getSlot(px::t, s::SlotNum)
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
function safe_getSlot(px::t, s::SlotNum)
    holding(lock(px), "safe_getSlot $(s)") do
        getSlot(px, s)
    end
end

function persistently(f::Function,  d::SQLite.DB, slot::Slot)
    f()
    syncSlot(d, slot)
end


######################
##     Tasklets     ##
######################

#=
#              Proposer functionality
=#
function AcceptPhase(px::t,slot_num::SlotNum, n::Int64, v::CriticalCommand, majority::Int64)
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

function Proposer(px::t, v::CriticalCommand, slot_num::SlotNum, ret = false)
    epoch = -1
    majority = div(length(peers(px)) +2, 2)
    has_produced = false
    while true
        pending = holding(lock(px), "Proposer While Loop $slot_num") do
            @debug "GETTING FATE"
            getSlot!(px, slot_num).fate == Pending
        end
        if !pending
            break
        else
            epoch += 1
            n = length(peers(px))*epoch + self(px) + 1
            chan = Base.Channel{PrepareReply}(length(peers(px)))# We don't want it to block ever
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
                            if ret && v_a != v_new && !has_produced
                                has_produced = true
                                produce(false)
                            end
                            v_a = v_new
                        end
                    end
                    if positives >= majority
                        a = AcceptPhase(px, slot_num, n, v_a, majority)
                        if a
                            if !has_produced
                                produce(true)
                            end
                            loop = false
                        end
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

end



function RPCServer(px::t)
    server = listen(port(px))
    @info "RPC Server Starting on port $(port(px)), node $(self(px))"
    while true
        conn = accept(server)
        @info "Incoming Connection on node $(self(px))"
        @async begin
            while true
                try
                    args = deserialize(conn)::BaseMessage
                    @info "RPCServer $(self(px)) Args: $args"
                    try
                        r = Handle(px, args)
                        @info "RPCServer $(self(px)) Response: $r"
                        serialize(conn,r)
                        flush(conn)
                    catch err
                        @debug "RPCServer $(self(px)) Error Handling:\n    $args\n     $err"
                        close(conn)
                        break
                    end
                catch err
                    @debug "RPCServer $(self(px)) Connection Failed With $err"
                    close(conn)
                    break
                    # finally
                    #     close(conn)
                end
            end
        end
    end
end
function LogCrawler(px::t)
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
                    @async Proposer(px, NoOp(), next.num) # TODO: Can we guarantee that command has a No_Op more idomatically?
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
        wait(Timer(1))
    end
end
function Forgettor(px::t, completed::Array{Int64,1})
    @debug "Forgetting up to $completed"
    holding(lock(px), "Forgettor") do
        updateCompleted(px, completed) # updates completed and forgotten fields and deletes elements from log
        d = db(px)
        syncForgot(px,d)

    end
end

function Heartbeat(px::t)
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
end
