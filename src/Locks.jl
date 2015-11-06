module Locks
export Lock, holding
type Lock
    sem::Base.Semaphore
    Lock() = new(Base.Semaphore(1))
end
function holding(f::Function, l::Lock, id)
    Base.acquire(l.sem)
    # print("$id HOLDING LOCK\n")
    f_ = f()
    Base.release(l.sem)
    # print("$id RELEASING LOCK\n")
    f_
end
function holding(f::Function, l::Lock)
    Base.acquire(l.sem)
    f_ = f()
    Base.release(l.sem)
    f_
end
export LevelTrigger, signal, waitSignal
type LevelTrigger
    condition::Lock
    val::Bool
    LevelTrigger() = new(Lock(), false)
end

function signal(l::LevelTrigger)
    holding(l.condition) do
        l.val = true
    end
end

function waitSignal(l::LevelTrigger)
    while (holding(l.condition) do
        l.val == false
        end)
        yield()

    end
end
function waitSignal(l::LevelTrigger, delay::Float64, totalDelay::Float64, timeout::Function)
    times = Int64(totalDelay/delay)
    for i =1:times
        if (holding(l.condition) do
            l.val 
            end)
            return
        end
        wait(Timer(delay))
    end
    timeout()
    while !(holding(l.condition) do
            l.val 
            end)
        wait(Timer(delay))
    end
    
end

end
