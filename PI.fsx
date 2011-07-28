#load "PI.fs"

open ParallelAlgorithms.PI

#time "on";;
let pi1 = serialPi();;
let pi2 = parallelPi();;
let pi3 = parallelPartitionerPi();;
let pi4 = serialLinqPi();;
let pi5 = parallelLinqPi();;