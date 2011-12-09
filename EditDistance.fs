namespace ParallelAlgorithms

// Note:
// Dynamic programming is still parallelizable in some cases.
// The curried version is lighter than the tuple version.
module WavefrontAlgorithm =
    open System.Threading.Tasks

    /// <summary>Process in parallel a matrix where every cell has a dependency on the cell above it and to its left.</summary> 
    /// <param name="numRows">The number of rows in the matrix.</param> 
    /// <param name="numColumns">The number of columns in the matrix.</param> 
    /// <param name="processRowColumnCell">The action to invoke for every cell, supplied with the row and column indices.</param>
    let wavefrontForCell numRows numColumns (processRowColumnCell: _ -> _ -> unit) =
        // Validate parameters 
        if numRows <= 0 then invalidArg "wavefront" "numRows"
        elif numColumns <=0 then invalidArg "wavefront" "numColumns"
        else
            // Store the previous row of tasks as well as the previous task in the current row 
            let prevTaskRow = Array.zeroCreate<Task> numColumns
            let mutable prevTaskCurrentRow = None

            let dependencies = Array.zeroCreate<Task> 2
            
            for r = 0 to numRows-1 do
                prevTaskCurrentRow <- None
                for c = 0 to numColumns-1 do
                   let task =
                       if r=0 && c=0 then
                            Task.Factory.StartNew(fun () -> processRowColumnCell r c)
                       elif r=0 || c=0 then
                            // Tasks in the left-most column depend only on the task above them, and 
                            // tasks in the top row depend only on the task to their left 
                            let antecedent = if c=0 then prevTaskRow.[0] else Option.get prevTaskCurrentRow
                            antecedent.ContinueWith(fun (p: Task) -> p.Wait(); processRowColumnCell r c)
                       else
                            dependencies.[0] <- Option.get prevTaskCurrentRow
                            dependencies.[1] <- prevTaskRow.[c]
                            Task.Factory.ContinueWhenAll(dependencies, fun ps -> Task.WaitAll(ps); processRowColumnCell r c)
                   prevTaskRow.[c] <- task
                   prevTaskCurrentRow <- Some task
            let t = Option.get prevTaskCurrentRow
            t.Wait()
                        
    /// <summary>Process in parallel a matrix where every cell has a dependency on the cell above it and to its left.</summary> 
    /// <param name="numRows">The number of rows in the matrix.</param> 
    /// <param name="numColumns">The number of columns in the matrix.</param> 
    /// <param name="numBlocksPerRow">Partition the matrix into this number of blocks along the rows.</param> 
    /// <param name="numBlocksPerColumn">Partition the matrix into this number of blocks along the columns.</param> 
    /// <param name="processBlock">The action to invoke for every block, supplied with the start and end indices of the rows and columns.</param> 
    let wavefront numRows numColumns numBlocksPerRow numBlocksPerColumn processBlock =
        if numRows <= 0 then invalidArg "wavefront" "numRows"
        elif numColumns <=0 then invalidArg "wavefront" "numColumns"
        elif numBlocksPerRow <= 0 || numBlocksPerRow > numRows then invalidArg "wavefront" "numBlocksPerRow"
        elif numBlocksPerColumn <= 0 || numBlocksPerColumn > numColumns then invalidArg "wavefront" "numBlocksPerColumn"
        else
            let rowBlockSize = numRows / numBlocksPerRow
            let columnBlockSize = numColumns / numBlocksPerColumn
            let inline processRowColumnCell row column =
                let startI = row * rowBlockSize
                let endI = if row < numBlocksPerRow - 1 then startI + rowBlockSize else numRows
                let startJ = column * columnBlockSize
                let endJ = if column < numBlocksPerColumn - 1 then startJ + columnBlockSize else numColumns
                processBlock startI endI startJ endJ
            wavefrontForCell numBlocksPerRow numBlocksPerColumn processRowColumnCell

module EditDistance =
    open System
    open WavefrontAlgorithm

    let editDistance (s1: string) (s2: string) =
        // Most of the time is actually spent on initializing dist matrix.
        // Array2D.zeroCreate < Array.create < Array.init (faster -> slower)
        let dist = Array2D.zeroCreate<int> (s1.Length+1) (s2.Length+1)
        for i = 0 to s1.Length do
            dist.[i, 0] <- i
        for j = 0 to s2.Length do
            dist.[0, j] <- j

        for i = 1 to s1.Length do
            for j = 1 to s2.Length do
                if s1.[i-1] = s2.[j-1] then dist.[i, j] <- dist.[i-1, j-1]
                else
                    dist.[i, j] <- 1 + min dist.[i-1, j] (min dist.[i, j-1] dist.[i-1, j-1])

        dist.[s1.Length, s2.Length]

    let editDistanceParallel (s1: string) (s2: string) =
        let dist = Array2D.zeroCreate<int> (s1.Length+1) (s2.Length+1)
        for i = 0 to s1.Length do
            dist.[i, 0] <- i
        for j = 0 to s2.Length do
            dist.[0, j] <- j
        
        let numBlocks = Environment.ProcessorCount * 4

        let inline processBlock startI endI startJ endJ =
            for i = startI+1 to endI do
                for j = startJ+1 to endJ do
                    if s1.[i-1] = s2.[j-1] then dist.[i, j] <- dist.[i-1, j-1]
                    else
                        dist.[i, j] <- 1 + min dist.[i-1, j] (min dist.[i, j-1] dist.[i-1, j-1])       
                             
        wavefront s1.Length s2.Length numBlocks numBlocks processBlock
        dist.[s1.Length, s2.Length]