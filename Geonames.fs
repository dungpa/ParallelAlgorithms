module ParallelAlgorithms.Geonames

open System
open System.IO
open Microsoft.FSharp.Collections

open System.Threading.Tasks

// Translate some basic parallel versions into F#; speedups are insignificant due to sequential process of reading files.
// The original article can be found at http://msdn.microsoft.com/en-us/magazine/gg535673.aspx

let nameColumn = 1
let countryColumn = 8
let elevationColumn = 16

let elevationBound = 7500

// Skip most fields, only extract elevation
let extractIntegerField(line: string, fieldNumber) =
    let mutable count = 0
    let mutable value = 0
    for ch in line do       
        if fieldNumber = count then 
            let digit = byte (int ch - 0x30)                        
            if digit >= 0uy && digit <= 9uy then                
                value <- 10*value + int digit                
            else
                count <- count + 1
        elif int ch = 0x09 then 
            count <- count + 1
    value   
    
let display (fs: seq<string>) =
    for l in fs do
        let fields = l.Split [|'\t'|]  
        Console.WriteLine("{0} ({1}m) - located in {2}", fields.[nameColumn], fields.[elevationColumn], fields.[countryColumn])

// Filter by elevation and sort descendingly
let searchLINQ(inFile) =
    File.ReadLines(Path.Combine(Environment.CurrentDirectory, inFile))
    |> Seq.choose (fun line ->  let elevation = extractIntegerField(line, elevationColumn)
                                if elevation > elevationBound then Some (-elevation, line) else None)
    |> Seq.sortBy fst
    |> Seq.map snd

// Use PSeq, a thin wrapper of PLINQ
let searchPLINQ(inFile, degreeOfParallelism) =
    File.ReadLines(Path.Combine(Environment.CurrentDirectory, inFile))
    |> PSeq.withDegreeOfParallelism degreeOfParallelism
    |> PSeq.choose (fun line ->  let elevation = extractIntegerField(line, elevationColumn)
                                 if elevation > elevationBound then Some (-elevation, line) else None)
    |> PSeq.sortBy fst
    |> Seq.map snd (* there's not much calculation => using Seq *)

// Sequential execution using a ResizeArray
let searchSequential(inFile) =
    let lines = File.ReadLines(Path.Combine(Environment.CurrentDirectory, inFile))

    let foundList = new ResizeArray<_>(100)
    for line in lines do
        let elevation = extractIntegerField(line, elevationColumn)
        if elevation > elevationBound then 
            foundList.Add((elevation, line))
    foundList.Sort(fun (el1, line1) (el2, line2) -> compare el2 el1)
    foundList |> Seq.map snd

// Parallel execution, use a global list to accummulate results
let searchParallel(inFile, degreeOfParallelism) =
    let lines = File.ReadLines(Path.Combine(Environment.CurrentDirectory, inFile))

    let foundList = new ResizeArray<_>(100)

    let options = new ParallelOptions(MaxDegreeOfParallelism = degreeOfParallelism)    
    let monitor = new Object()

    Parallel.ForEach( 
        lines, options, (fun () -> new ResizeArray<_>(100)),
        (fun line _ (taskFoundList: ResizeArray<_>) ->  let elevation = extractIntegerField(line, elevationColumn)
                                                        if elevation > elevationBound then 
                                                            taskFoundList.Add((elevation, line))
                                                        taskFoundList),
        (fun localFoundList -> lock (monitor) (fun () -> foundList.AddRange(localFoundList)))) |> ignore

    foundList.Sort(fun (el1, line1) (el2, line2) -> compare el2 el1)    
    foundList |> Seq.map snd

