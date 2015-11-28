using Rock
peers = [(Rock.Hostname("localhost"), Rock.Port(8000)),
         (Rock.Hostname("localhost"), Rock.Port(8001)),
         (Rock.Hostname("localhost"), Rock.Port(8002))]
type C <: Rock.ExternalCriticalCommand
    a
end
function t(px, d, c)
@show c
end

i = parse(Int64, ARGS[1])
if length(ARGS) >1
    Rock.command(Rock.client("localhost", peers[i][2]), C(ARGS[2]))
else
    c = Rock.Context(peers[i][2], peers, t, AbstractString("sqlite$i.db"))
    n  =Rock.Instance(c,i)
    Rock.start(n)
end
