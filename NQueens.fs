module ParallelAlgorithms.NQueens

// Count all solutions of an NQueens problem.
// Translate from C# version at http://blogs.msdn.com/b/pfxteam/archive/2009/12/09/9934811.aspx#NQueens

// Lessons learned:
// 1. Use classes and array mutation inside, side effect is kept inside the class.
// 2. Partially solve problems and use those solutions for parallel execution.
// 3. When tasks are heavy enough, a small number of tasks still gives a good speedup.

open System

open System.Collections.Concurrent
open System.Threading.Tasks
    
type Board(initialSize: int) =
    let size = initialSize // size of the chessboard
    let mutable col = 0 // Column with the smallest index that does not contain a queen, 0 <= col < size
    let rows = Array.create size false // rows[i] = does row i contain a queen?
    let fwDiagonals = Array.create (2*size-1) false // fwDiagonals[i] = does forward diagonal i contain a queen?
    let bwDiagonals = Array.create (2*size-1) false // bwDiagonals[i] = does backward diagonal i contain a queen?

    member x.safe(row) =
        not (rows.[row] || fwDiagonals.[row + col] 
                        || bwDiagonals.[row - col + size - 1])

    member x.placeQueen(row) =
        rows.[row] <- true
        fwDiagonals.[row + col] <- true
        bwDiagonals.[row - col + size - 1] <- true
        col <- col+1
        
    member x.removeQueen(row) =
        col <- col-1
        rows.[row] <- false
        fwDiagonals.[row + col] <- false
        bwDiagonals.[row - col + size - 1] <- false

    member x.countSolutions() =
        if col = size then 1
        else
            let mutable answer = 0
            for row in 0..size-1 do
                if x.safe(row) then
                    x.placeQueen(row)
                    answer <- answer + x.countSolutions()
                    x.removeQueen(row)
            answer

let countSequential1 size =
    let board = new Board(size)
    board.countSolutions()

// Align with the corresponding parallel version.
let countSequential2 size =
    let n = (size-1)*(size-2)
    let boards = Array.init n (fun _ -> new Board(size)) 
    let mutable count = 0
    for i in 0..size-1 do
        for j in 0..size-1 do
            if abs(i-j) > 1 then
                boards.[count].placeQueen(i)
                boards.[count].placeQueen(j)
                count <- count+1

    let mutable sum = 0
    for i in 0..n-1 do
        sum <- sum + boards.[i].countSolutions()
    sum

// Place the first queen beforehand for parallel execution.
let countParallel1 size =
    let boards = Array.init size (fun _ -> new Board(size)) 
    for i in 0..size-1 do
        boards.[i].placeQueen(i)

    boards |> Array.Parallel.map (fun b -> b.countSolutions()) |> Array.sum
//    let solutions = Array.zeroCreate size
//    Parallel.For(0, size,
//        fun i -> solutions.[i] <- boards.[i].countSolutions()) |> ignore
//    Array.sum solutions

// Place two first queens beforehand for parallel execution.
let countParallel2 size =
    let n = (size-1)*(size-2)
    let boards = Array.init n (fun _ -> new Board(size)) 
    let mutable count = 0
    for i in 0..size-1 do
        for j in 0..size-1 do
            if abs(i-j) > 1 then // safe to place two queens together
                boards.[count].placeQueen(i)
                boards.[count].placeQueen(j)
                count <- count+1
    
    boards |> Array.Parallel.map (fun b -> b.countSolutions()) |> Array.sum
//    let solutions = Array.zeroCreate n
//    Parallel.For(0, n,
//        fun i -> solutions.[i] <- boards.[i].countSolutions()) |> ignore
//    Array.sum solutions