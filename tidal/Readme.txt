Bridges2\ocean\projects\bcs200002p\ewhite12\MP2029\ICMv24\S00\

==================
Hydro_only:
x G100 - Hydro_only dt=15s with corrected % attributes                                               - crashed 2019 Day193 Comp741
x G102 - Hydro_only dt=15s same as G100 except without type8/9 links                                 - crashed 2019 Day193 comp741
x G104 - Hydro_only same as G100 except dt=5s                                                        - Done!

------revised links 01/17/2025
G110 - Hydro_only dt=15s same as G100 except with revised links 01/17/2025                         - crashed 2019 Day 193 Comp741
G111 - Hydro_only dt=15s same as G110 except without type8/9 links                                 - Done!
G112 - Hydro_only dt=15s same as G110 except change link Manning's n=0.02 from original <=0.01     - Done!

G113 - Hydro_only dt=5s same as G110 except reduced dt
G114 - Hydro_only dt=5s same as G112 except reduced dt

===================
Full ICM:
G106 - same as G100 "true" history (with projects) dt=15s            - hotstart 2021      
G107 - same as G100 "alternative" history (no project) dt=15s        - Done!

------
Full ICM:
x G108 - "true" history (with projects) dt=15s - MP23 cells/links      - crashed 2008
x G109 - "alternative" history (no project) dt=15s - MP23 cells/links  - crashed 2008

------
OMP Parallel Tests:
G437 - 3 thread
G438 - OMP build, no directives 1 thread
G439 - 4 thread
G440 - 2 thread
G441 - 1 thread