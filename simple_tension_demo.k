*KEYWORD
*TITLE
Simple Tension Test Demo for LS-DYNA
$
$ Control Parameters
$
*CONTROL_TERMINATION
     0.001
$
*CONTROL_TIMESTEP
       0.0       0.9         0       0.0       0.0         0         0         0
$
$ Output Control
$
*DATABASE_BINARY_D3PLOT
     1.E-5
$
$ Material - Elastic Steel
$
*MAT_ELASTIC
$      MID        RO         E        PR
         1    7.85E-9     2.1E5       0.3
$
$ Section
$
*SECTION_SOLID
         1         1
$
$ Part
$
*PART
Tensile Specimen
         1         1         1
$
$ Nodes - Simple Bar (2x2x10 units)
$
*NODE
         1       0.0       0.0       0.0
         2       2.0       0.0       0.0
         3       2.0       2.0       0.0
         4       0.0       2.0       0.0
         5       0.0       0.0      10.0
         6       2.0       0.0      10.0
         7       2.0       2.0      10.0
         8       0.0       2.0      10.0
$
$ Element
$
*ELEMENT_SOLID
         1        1       1       2       3       4       5       6       7       8
$
$ Boundary Conditions - Fix bottom
$
*BOUNDARY_SPC_NODE
         1       0         1         1         1         0         0         0
         2       0         1         1         1         0         0         0
         3       0         1         1         1         0         0         0
         4       0         1         1         1         0         0         0
$
$ Load - Prescribed Motion on top
$
*BOUNDARY_PRESCRIBED_MOTION_NODE
         5         1         2      1000.0       0.0         1       0.0       0.0
         6         1         2      1000.0       0.0         1       0.0       0.0
         7         1         2      1000.0       0.0         1       0.0       0.0
         8         1         2      1000.0       0.0         1       0.0       0.0
$
$ Load Curve
$
*DEFINE_CURVE
         1         0       1.0       1.0       0.0       0.0
       0.0       0.0
     0.001       1.0
$
*END