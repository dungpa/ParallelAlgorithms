module ParallelAlgorithms.BootyDivision

open System
open System.Threading.Tasks

// Parallelization of booty division problem.
// See details of discussion at http://www.devx.com/dotnet/Article/39219/0/pdo/D9BF62877FACB1F6EDB86291994B1873:8716/page/3.
type BootyDivider(_itemValues) =
    let itemValues = Array.copy _itemValues // Not really necessary.
    let numItems = Array.length itemValues

    // Important values
    let size = 2
    let mutable depth = 0

    // Could use bool array, but use byte array to extend for more than 2 dividers.
    let path = Array.create numItems 255uy
    let bestPath = Array.create numItems 255uy

    let totalValues = Array.create size 0    
    let bestTotalValues = Array.create size 0 

    let mutable totalUnassigned = Array.sum itemValues
    let mutable bestDifference = Int32.MaxValue

    // Divide nth item to index-th person.
    member x.select(index) =        
        path.[depth] <- byte(index)
        totalValues.[index] <- totalValues.[index] + itemValues.[depth]
        depth <- depth + 1

    member x.deselect(index) =        
        depth <- depth - 1
        path.[depth] <- 0uy
        totalValues.[index] <- totalValues.[index] - itemValues.[depth]

    member x.divide() =
        if depth >= numItems then
            if abs(totalValues.[0] - totalValues.[1]) < bestDifference then
                Array.Copy(totalValues, bestTotalValues, size)
                bestDifference <- abs(totalValues.[0] - totalValues.[1])
            else ()            
        else            
            for i in 0..1 do
                x.select(i)
                x.divide() |> ignore
                x.deselect(i)                    
        bestDifference

    member x.selectBB(index) =
        totalUnassigned <- totalUnassigned - itemValues.[depth]
        x.select(index)

    member x.deselectBB(index) =        
        x.deselect(index)
        totalUnassigned <- totalUnassigned + itemValues.[depth]

    // Using branch and bound method to prune unused branches.
    member x.divideBB() =
        if depth >= numItems then
            if abs(totalValues.[0] - totalValues.[1]) < bestDifference then
                Array.Copy(totalValues, bestTotalValues, size)
                bestDifference <- abs(totalValues.[0] - totalValues.[1])
            else ()            
        elif abs(totalValues.[0] - totalValues.[1]) - totalUnassigned <= bestDifference then                       
            for i in 0..1 do
                x.selectBB(i)
                x.divideBB() |> ignore
                x.deselectBB(i)                    
        else ()
        bestDifference

    member x.preassign(level, threadId) =
        for l in 1..level do
            if threadId &&& (pown 2 l) <> 0 then
                path.[depth] <- 1uy
                totalValues.[1] <- totalValues.[1] + itemValues.[level-l]
                depth <- depth + 1        
            else
                path.[depth] <- 0uy
                totalValues.[0] <- totalValues.[0] + itemValues.[level-l]
                depth <- depth + 1  

    // Preassign the divider with some configurations.
    member x.preassignBB(level, threadId) =
        for l in 1..level do
            if threadId &&& (pown 2 l) <> 0 then
                path.[depth] <- 1uy
                totalValues.[1] <- totalValues.[1] + itemValues.[depth]
                totalUnassigned <- totalUnassigned - itemValues.[depth]
                depth <- depth + 1        
            else
                path.[depth] <- 0uy
                totalValues.[0] <- totalValues.[0] + itemValues.[depth]
                totalUnassigned <- totalUnassigned - itemValues.[depth]
                depth <- depth + 1        

let solve(itemValues) =
    let bd = new BootyDivider(itemValues)
    bd.divide()

// This version is aligned with the parallel version.
let solve2(itemValues, level) =
    let count = pown 2 (level-1)
    // Setup different booty dividers with different configurations.
    let bds = Array.init count (fun _ -> new BootyDivider(itemValues))
    for i in 0..count-1 do
        bds.[i].preassign(level, i)

    let solutions = Array.zeroCreate count
    for i in 0..count-1 do
        solutions.[i] <- bds.[i].divide()
    Array.min solutions

let solveParallel(itemValues, level) =
    let count = pown 2 level
    // Setup different booty dividers with different configurations.
    let bds = Array.init count (fun _ -> new BootyDivider(itemValues))
    for i in 0..count-1 do
        bds.[i].preassign(level, i)

    let solutions = Array.zeroCreate count
    Parallel.For(0, count,
        fun i -> solutions.[i] <- bds.[i].divide()) |> ignore
    Array.min solutions

let solveBB(itemValues) =
    let bd = new BootyDivider(itemValues)
    bd.divideBB()

let solveBBParallel(itemValues, level) =
    let count = pown 2 level
    // Setup different booty dividers with different configurations.
    let bds = Array.init count (fun _ -> new BootyDivider(itemValues))
    for i in 0..count-1 do
        bds.[i].preassignBB(level, i)

    let solutions = Array.zeroCreate count
    Parallel.For(0, count,
        fun i -> solutions.[i] <- bds.[i].divideBB()) |> ignore
    Array.min solutions