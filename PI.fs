module ParallelAlgorithms.PI

// Implement parallel PI by various construct in F#
// Translate from C# version at http://blogs.msdn.com/b/pfxteam/archive/2009/12/09/9934811.aspx#ComputePi

open System
open System.Linq
open System.Threading.Tasks
open System.Collections.Concurrent

let NUM_STEPS = 100000000
let steps = 1.0 / (float NUM_STEPS)

let serialPi() =
    let rec computeUtil(i, acc) =
        if i = 0 then acc * steps
        else
            let x = (float i + 0.5) * steps
            computeUtil (i-1, acc + 4.0 / (1.0 + x * x))
    computeUtil(NUM_STEPS, 0.0)

let parallelPi() = 
    let sum = ref 0.0
    let monitor = new Object()
    Parallel.For( 
        0, NUM_STEPS, new ParallelOptions(),
        (fun () -> 0.0),
        (fun i loopState (local:float) ->
            let x = (float i + 0.5) * steps
            local + 4.0 / (1.0 + x * x)
            ),
        (fun local -> lock (monitor) (fun () -> sum := !sum + local))) |> ignore
    !sum * steps

let parallelPartitionerPi() = 
    let size = Environment.ProcessorCount * 10
    let rangeSize = NUM_STEPS / size
    let partitions = Partitioner.Create(0, NUM_STEPS, if rangeSize >= 1 then rangeSize else 1)
    let output = Array.zeroCreate size
    Parallel.ForEach(
        partitions, new ParallelOptions(),            
        (fun (min, max) loopState ->
            let local = ref 0.0
            for i in min .. max - 1 do
                let x = (float i + 0.5) * steps
                local := !local + 4.0 / (1.0 + x * x)
            output.[min/rangeSize] <- !local
            )) |> ignore
    (Array.sum output) * steps

let inline sqr x = x * x

let serialLinqPi() =
    (Enumerable
        .Range(0, NUM_STEPS)          
        .Select(fun i -> 4.0 / (1.0 + sqr ((float i + 0.5) * steps)))
        .Sum()) * steps

let parallelLinqPi() =
    (ParallelEnumerable
        .Range(0, NUM_STEPS)          
        .Select(fun i -> 4.0 / (1.0 + sqr ((float i + 0.5) * steps)))
        .Sum()) * steps