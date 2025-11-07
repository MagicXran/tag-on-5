*KEYWORD
$
$ LS-DYNA Ball Impact Demo
$ Simple demonstration of a ball dropping on a rigid plate
$ Created for ANSYS 2022 R2 LS-DYNA validation
$
*TITLE
Ball Impact on Rigid Plate - Simple Demo
$
$ Control Cards
$
*CONTROL_TERMINATION
$   ENDTIM    ENDCYC     DTMIN    ENDENG    ENDMAS
     0.005         0       0.0       0.0       0.0
$
*CONTROL_TIMESTEP
$  DTINIT    TSSFAC      ISDO    TSLIMT     DT2MS      LCTM     ERODE     MS1ST
       0.0       0.9         0       0.0       0.0         0         0         0
$
*CONTROL_ENERGY
$     HGEN      RWEN    SLNTEN     RYLEN
         2         2         2         2
$
*CONTROL_OUTPUT
$   NPOPT    NEECHO    NREFUP    IACCOP     OPIFS    IPNINT    IKEDIT
         0         0         0         0       0.0         0       100
$
$ Database Cards for Output
$
*DATABASE_BINARY_D3PLOT
$       DT      LCDT      BEAM     NPLTC
     1.E-4         0         0         0
$
*DATABASE_EXTENT_BINARY
$    NEIPH     NEIPS    MAXINT    STRFLG    SIGFLG    EPSFLG    RLTFLG    ENGFLG
         0         0         3         0         1         1         0         1
$   CMPFLG    IEVERP    BEAMIP     DCOMP      SHGE     STSSZ    N3THDT   IALEMAT
         0         0         0         1         1         1         2         1
$  NINTSLD   PKP_SEN      SCLP    UNUSED     MSSCL
         0         0       1.0                   0
$
$ Material Definition
$
*MAT_ELASTIC
$      MID        RO         E        PR        DA        DB  NOT USED
         1    7850.0     2.1E5       0.3       0.0       0.0       0.0
$
*MAT_RIGID
$      MID        RO         E        PR         N    COUPLE         M     ALIAS
         2    7850.0     2.1E5       0.3       0.0       0.0       0.0
$      CMO      CON1      CON2
       1.0         7         7
$
$ Section Properties
$
*SECTION_SOLID
$    SECID    ELFORM       AET
         1         1         0
$
$ Part Definitions
$
*PART
Ball
$      PID     SECID       MID     EOSID      HGID      GRAV    ADPOPT      TMID
         1         1         1         0         0         0         0         0
$
*PART
Rigid Plate
$      PID     SECID       MID     EOSID      HGID      GRAV    ADPOPT      TMID
         2         1         2         0         0         0         0         0
$
$ Node Definitions - Ball (Simple 8-node cube representing ball)
$
*NODE
$      NID               X               Y               Z      TC      RC
         1            -1.0            -1.0             5.0       0       0
         2             1.0            -1.0             5.0       0       0
         3             1.0             1.0             5.0       0       0
         4            -1.0             1.0             5.0       0       0
         5            -1.0            -1.0             7.0       0       0
         6             1.0            -1.0             7.0       0       0
         7             1.0             1.0             7.0       0       0
         8            -1.0             1.0             7.0       0       0
$
$ Node Definitions - Rigid Plate
$
       101           -10.0           -10.0             0.0       0       0
       102            10.0           -10.0             0.0       0       0
       103            10.0            10.0             0.0       0       0
       104           -10.0            10.0             0.0       0       0
       105           -10.0           -10.0            -1.0       0       0
       106            10.0           -10.0            -1.0       0       0
       107            10.0            10.0            -1.0       0       0
       108           -10.0            10.0            -1.0       0       0
$
$ Element Definitions - Ball
$
*ELEMENT_SOLID
$      EID      PID      N1      N2      N3      N4      N5      N6      N7      N8
         1        1       1       2       3       4       5       6       7       8
$
$ Element Definitions - Rigid Plate
$
       101        2     101     102     103     104     105     106     107     108
$
$ Initial Velocity for Ball
$
*INITIAL_VELOCITY
$     NSID    NSIDEX     BOXID
         1         0         0
$       VX        VY        VZ       VXR       VYR       VZR
       0.0       0.0   -1000.0       0.0       0.0       0.0
$
$ Node Set for Ball
$
*SET_NODE_LIST
$      SID       DA1       DA2       DA3       DA4    SOLVER
         1       0.0       0.0       0.0       0.0      MECH
$     NID1      NID2      NID3      NID4      NID5      NID6      NID7      NID8
         1         2         3         4         5         6         7         8
$
$ Boundary Conditions - Fix Rigid Plate
$
*BOUNDARY_SPC_SET
$    NSID       CID      DOFX      DOFY      DOFZ     DOFRX     DOFRY     DOFRZ
         2         0         1         1         1         1         1         1
$
$ Node Set for Rigid Plate
$
*SET_NODE_LIST
$      SID       DA1       DA2       DA3       DA4    SOLVER
         2       0.0       0.0       0.0       0.0      MECH
$     NID1      NID2      NID3      NID4      NID5      NID6      NID7      NID8
       101       102       103       104       105       106       107       108
$
$ Contact Definition
$
*CONTACT_AUTOMATIC_SURFACE_TO_SURFACE
$     SSID      MSID     SSTYP     MSTYP    SBOXID    MBOXID       SPR       MPR
         3         4         3         3         0         0         0         0
$       FS        FD        DC        VC       VDC    PENCHK
       0.2       0.1       0.0       0.0       0.0         0
$
$ Part Set for Ball
$
*SET_PART_LIST
$      SID       DA1       DA2       DA3       DA4    SOLVER
         3       0.0       0.0       0.0       0.0      MECH
$     PID1      PID2      PID3      PID4      PID5      PID6      PID7      PID8
         1
$
$ Part Set for Rigid Plate
$
*SET_PART_LIST
$      SID       DA1       DA2       DA3       DA4    SOLVER
         4       0.0       0.0       0.0       0.0      MECH
$     PID1      PID2      PID3      PID4      PID5      PID6      PID7      PID8
         2
$
*END