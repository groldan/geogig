# QA Workflow - CLI based

## Overview

The `Gold` repository has a `master` branch, to which only the Admin can commit to,
User Gabe has his own clone of the gold repository called `integration`, and acts as an integrator for work contributed by users `hannah` and `dave`, who do field work.

The workflow consists of the following steps:
- Gabe synchronizes `integration` with `Gold`, and has Hanna's and Dave's repositories as remotes
- Hannah is sent to the field to update points of interest in the St. Louis area. For which she grabs a geopackage export of her `integration` repository clone, performs the edits in the field, and once done imports the changes from the geopackage to her repository and notifies Gabe that her work is ready for integration.
 * Add St. Louis' Gateway Arch at coordinate -10039323.16,4668052.83 (EPSG:3857), intentionally with a typo "Arc" instead of "Arch"
 * Change `type` attribute of feature `points/358035969` from `school` to `academy`
- Dave is commanded to load the new poinst of interest from the 2015-1016 osm dump. He creates a `1601-osm-import` branch on his clone, imports the new data, and performs basic verifications. Then notifies Gabe that  his `1601-osm-import` is ready.
- Gabe pulls in Hannah's changes onto `integration` and reverts her commit because there's a mistake to fix (for the sake of this demo, just a typo)
- Gabe pulls in Dave's changes onto `integration`. There's a merge conflict on feature `points/358035969` as both Hannah and Dave changed the same attribute for that feature. Gabe resolves the conflict choosing Hanna's version as it's more current than the 2016 osm import, and sends them to `Gold`.


## Get some data

```
$ mkdir -p /data/repositories/capabilities_demo
$ cd /data/repositories/capabilities_demo
$ mkdir missouri-1501 missouri-1601
$ wget https://s3.amazonaws.com/geogig-test-data/osm/geofabrik/north-america/us/missouri-150101.shp.zip -O missouri-1501/missouri-150101.shp.zip
$ wget https://s3.amazonaws.com/geogig-test-data/osm/geofabrik/north-america/us/missouri-160101.shp.zip -O missouri-1601/missouri-160101.shp.zip
$ cd missouri-1501/
$ unzip missouri-150101.shp.zip 
  inflating: buildings.shp           
  inflating: landuse.shp             
  inflating: natural.shp             
  inflating: places.shp              
  inflating: points.shp              
  inflating: railways.shp            
  inflating: roads.shp               
  inflating: waterways.shp           
...
$ cd ../missouri-1601/
$ unzip missouri-160101.shp.zip 
  inflating: buildings.shp           
  inflating: landuse.shp             
  inflating: natural.shp             
  inflating: places.shp              
  inflating: points.shp              
  inflating: railways.shp            
  inflating: roads.shp               
  inflating: waterways.shp           
$ cd ..
$ pwd
/data/repositories/capabilities_demo
$ 
```

## Initialize "gold" repository

```
$ gig init gold
$ cd gold
$ export PS1="gold\$ "
gold$ gig config user.name admin
gold$ gig config user.email admin@demo.com
gold$ gig status
# On branch master
nothing to commit (working directory clean)
```

## Import inital datasets onto gold repo

Switch to the "gold" terminal and:

```
gold$ gig shp describe ../missouri-1501/buildings.shp
Fetching table...
Table : buildings
----------------------------------------
	Property  : osm_id
	Type      : String
----------------------------------------
	Property  : name
	Type      : String
----------------------------------------
	Property  : type
	Type      : String
----------------------------------------
	Property  : the_geom
	Type      : MultiPolygon
----------------------------------------
gold$ gig shp import --fid-attrib osm_id ../missouri-1501/buildings.shp
Importing from shapefile ../missouri-1501/buildings.shp
Importing buildings        (1/1)... 
24,276 features inserted in 800.2 ms
Building final tree buildings...
24,276 features tree built in 151.8 ms
../missouri-1501/buildings.shp imported successfully.
gold$ gig add
Counting unstaged elements...24277
Staging changes...
24,276 features and 1 trees staged for commit
0 features and 0 trees not staged for commit
gold$ gig commit -m "Initial import of Missouri buildings from 2015-01 osm dump"
[c0591e2e2e762018787633c7597dafb9b5d532b8] Initial import of Missouri buildings from 2015-01 osm dump
Committed, counting objects...24276 features added, 0 changed, 0 deleted.
gold$ gig ls
Root tree/
	buildings/
gold$ gig ls-tree -s
buildings 24,276
gold$ 
gold$ gig shp import --fid-attrib osm_id ../missouri-1501/points.shp
...
../missouri-1501/points.shp imported successfully.
gold$ gig add
...
44,118 features and 1 trees staged for commit
gold$ gig commit -m "Initial import of Missouri points of interest from 2015-01 osm dump"
[5fa95e5ccb49311679d388b59339fb396bc5a22f] Initial import of Missouri points of interest from 2015-01 osm dump
Committed, counting objects...44118 features added, 0 changed, 0 deleted.
gold$
gold$ gig shp import --fid-attrib osm_id ../missouri-1501/roads.shp
...
../missouri-1501/roads.shp imported successfully.
gold$ gig add
...
524,562 features and 1 trees staged for commit
gold$ gig commit -m "Initial import of Missouri roads from 2015-01 osm dump"
[f359c7973d3ca85a717559d6fbab070bf15b2e9e] Initial import of Missouri roads from 2015-01 osm dump
Committed, counting objects...524562 features added, 0 changed, 0 deleted.
gold$ gig log
Commit:  f359c7973d3ca85a717559d6fbab070bf15b2e9e
Author:  admin <admin@demo.com>
Date:    (1 minutes ago) 2017-06-04 22:05:15 -0300
Subject: Initial import of Missouri roads from 2015-01 osm dump

Commit:  5fa95e5ccb49311679d388b59339fb396bc5a22f
Author:  admin <admin@demo.com>
Date:    (3 minutes ago) 2017-06-04 22:02:49 -0300
Subject: Initial import of Missouri points of interest from 2015-01 osm dump

Commit:  c0591e2e2e762018787633c7597dafb9b5d532b8
Author:  admin <admin@demo.com>
Date:    (7 minutes ago) 2017-06-04 21:59:08 -0300
Subject: Initial import of Missouri buildings from 2015-01 osm dump
gold$ gig ls-tree -s
buildings 24,276
points 44,118
roads 524,562
gold$ gig ls-tree -s HEAD~
buildings 24,276
points 44,118
gold$ gig ls-tree -s HEAD~2
buildings 24,276
gold$ gig ls-tree -s HEAD~3
Invalid reference: HEAD~3
gold$ 
gold$ gig branch QA
Created branch refs/heads/QA
```

## Create clone of gold repo

On another terminal:

```
$ cd /data/repositories/capabilities_demo
$ export PS1="gabe\$ "
gabe$ gig clone gold integration
Cloning into 'integration'...
Fetching objects from refs/heads/master
100%
Done.
gabe$ cd integration
gabe$ gig log
Commit:  f359c7973d3ca85a717559d6fbab070bf15b2e9e
Author:  admin <admin@demo.com>
Date:    (36 minutes ago) 2017-06-04 22:05:15 -0300
Subject: Initial import of Missouri roads from 2015-01 osm dump

Commit:  5fa95e5ccb49311679d388b59339fb396bc5a22f
Author:  admin <admin@demo.com>
Date:    (38 minutes ago) 2017-06-04 22:02:49 -0300
Subject: Initial import of Missouri points of interest from 2015-01 osm dump

Commit:  c0591e2e2e762018787633c7597dafb9b5d532b8
Author:  admin <admin@demo.com>
Date:    (42 minutes ago) 2017-06-04 21:59:08 -0300
Subject: Initial import of Missouri buildings from 2015-01 osm dump

gabe$ gig remote list -v
origin file:/data/repositories/capabilities_demo/gold/ (fetch)
origin file:/data/repositories/capabilities_demo/gold/ (push)
gabe$ 
gabe$ gig config user.name gabe 
gabe$ gig config user.email gabe@demo.com
gabe$ gig diff origin/master
No differences found
gabe$ 
```

## Create Hannah's repo

As Hannah:

```
hannah$ cd /data/repositories/capabilities_demo
hannah$ gig clone integration hannah
Cloning into 'hannah'...
Fetching objects from refs/heads/master
Done.
hannah$ cd hannah/
hannah$ gig config user.name hannah 
hannah$ gig config user.email hannah@demo.com
```

## Create Dave's repo

As Dave:

```
dave$ cd /data/repositories/capabilities_demo
dave$ gig clone integration dave
Cloning into 'dave'...
Fetching objects from refs/heads/master
Done.
dave$ cd dave
dave$ gig config user.name dave 
dave$ gig config user.email dave@demo.com
```

## Hannah's field work

As Hannah:

* Export geopackage
```
hannah$ gig branch fieldwork
Created branch refs/heads/fieldwork
hannah$ gig checkout fieldwork
Switched to branch 'fieldwork'
hannah$ gig branch
* fieldwork
  master
hannah$ 
hannah$ gig geopkg export -i -D points.gpkg points points
Exporting from points to points... 
100%
points exported successfully to points
Exporting repository metadata from 'HEAD:points' (commit f359c7973d3ca85a717559d6fbab070bf15b2e9e)...
Creating audit metadata for table 'points'
hannah$ ls -l
total 7572
-rw-r--r-- 1 groldan groldan 7748608 jun  5 12:44 points.gpkg
```
* Open `points.gpkg` in QGIS and dd St. Louis' Gateway Arch at coordinate `-10039323.16,4668052.83` (EPSG:3857), intentionally with a typo "Arc" instead of "Arch"

```
hannah$ gig geopkg pull -D points.gpkg -m "Added missing points of interest in the St. Louis area" -t points

Importing changes to table points onto feature tree points...
Import successful.
Changes committed and merge at 8907305347d4f922f232b3d3fe75fa1e8b6c089c
hannah$ gig log --oneline
8907305347d4f922f232b3d3fe75fa1e8b6c089c Added missing points of interest in the St. Louis area
f359c7973d3ca85a717559d6fbab070bf15b2e9e Initial import of Missouri roads from 2015-01 osm dump
5fa95e5ccb49311679d388b59339fb396bc5a22f Initial import of Missouri points of interest from 2015-01 osm dump
c0591e2e2e762018787633c7597dafb9b5d532b8 Initial import of Missouri buildings from 2015-01 osm dump
[3;J
hannah$ gig branch
* fieldwork
  master
hannah$ gig diff master
00000000... 39b99ec7... 00000000... 76fc6185...   A  points/fid--5586c310_15c78f479c3_-8000
the_geom	POINT (-90.18477644191731 38.624693351094884)
osm_id	
timestamp	
name	Gateway Arc
type	
```

* Edit a point of interest that's also changed by Dave's import to create a conflict when they are merged

```
hannah$ gig ql "update points set type = 'academy' where \"@id\" = '358035969'"
Updated 1 features
hannah$ 
hannah$ gig status
# On branch fieldwork
# Changes not staged for commit:
#   (use "geogig add <path/to/fid>..." to update what will be committed
#   (use "geogig checkout -- <path/to/fid>..." to discard changes in working directory
#
#      modified  points
#      modified  points/1157178123
# 2 total.
hannah$ gig diff
39b99ec7... 39b99ec7... fc6a2bde... 8b5faa96...   M  points/358035969
type: level_crossing -> academy
hannah$ gig add
Counting unstaged elements...2
Staging changes...
1 features and 1 trees staged for commit
0 features and 0 trees not staged for commit
hannah$ gig show points/1157178123

ID:  8b5faa96d9db1da2977f4b4af1afb1a9b151e2c8
FEATURE TYPE ID:  39b99ec7321f00a5682798c0e05f9685b0acdb9b

ATTRIBUTES 
----------  
the_geom: POINT (-94.6096202 39.1109274)
osm_id: 1157178123
timestamp: 2011-02-17T06:08:25Z
name: 
type: academy

hannah$ gig commit -m "Update type of Boundless academy"
100%
[16209fb0cd16b10df0aed5f89d633549fdb6b2ab] Update type of Boundless academy
Committed, counting objects...0 features added, 1 changed, 0 deleted.

```

* Send changes to the `integration` repository

```
hannah$ gig branch
* fieldwork
  master
hannah$ gig push origin fieldwork

Uploading objects to fieldwork
hannah$ 
```

## Integrate Hanna's changes

* Gabe's working on the `integration` repository

```
gabe$ gig merge fieldwork
Checking for possible conflicts...
Merging commit e8e7821936ac888b87767dca0d276e2d9fb36b4c
Conflicts: 0, merged: 0, unconflicted: 3
[e8e7821936ac888b87767dca0d276e2d9fb36b4c] Update type of Boundless academy
Committed, counting objects...0 features added, 1 changed, 0 deleted.
gabe$ gig log
Commit:  e8e7821936ac888b87767dca0d276e2d9fb36b4c
Author:  hannah <hannah@demo.com>
Date:    (42 seconds ago) 2017-06-05 13:16:33 -0300
Subject: Update type of Boundless academy

Commit:  abe21369b813f2fce3093760e52f5e627005daf0
Author:  hannah <hannah@demo.com>
Date:    (1 minutes ago) 2017-06-05 16:16:13 +0000
Subject: Added missing points of interest in the St. Louis area

Commit:  f359c7973d3ca85a717559d6fbab070bf15b2e9e
Author:  admin <admin@demo.com>
Date:    (15 hours ago) 2017-06-04 22:05:15 -0300
Subject: Initial import of Missouri roads from 2015-01 osm dump

Commit:  5fa95e5ccb49311679d388b59339fb396bc5a22f
Author:  admin <admin@demo.com>
Date:    (15 hours ago) 2017-06-04 22:02:49 -0300
Subject: Initial import of Missouri points of interest from 2015-01 osm dump

Commit:  c0591e2e2e762018787633c7597dafb9b5d532b8
Author:  admin <admin@demo.com>
Date:    (15 hours ago) 2017-06-04 21:59:08 -0300
Subject: Initial import of Missouri buildings from 2015-01 osm dump

```
### Revert change

```
gabe$ gig diff HEAD~2 HEAD~1
00000000... 39b99ec7... 00000000... 76fc6185...   A  points/fid--72ed05ec_15c790ad9d6_-8000
the_geom	POINT (-90.18477644191731 38.624693351094884)
osm_id	
timestamp	
name	Gateway Arc
type	

gabe$ gig revert HEAD~1
```

```
hannah$ gig checkout master
Switched to branch 'master'
hannah$ gig pull origin master
Fetching objects from refs/heads/master
100%
From file:/data/repositories/capabilities_demo/integration/
   f359c797..da26052f     master -> refs/remotes/origin/master
Features Added: 0 Removed: 0 Modified: 1
hannah$ gig log
Commit:  da26052f12a85c5190a6435d21b399d6f285f27a
Author:  gabe <gabe@geogig.org>
Date:    (1 minutes ago) 2017-06-05 13:20:31 -0300
Subject: Revert commit abe21369b813f2fce3093760e52f5e627005daf0, there's a typo in the Arch's name

Commit:  e8e7821936ac888b87767dca0d276e2d9fb36b4c
Author:  hannah <hannah@demo.com>
Date:    (5 minutes ago) 2017-06-05 13:16:33 -0300
Subject: Update type of Boundless academy

Commit:  abe21369b813f2fce3093760e52f5e627005daf0
Author:  hannah <hannah@demo.com>
Date:    (6 minutes ago) 2017-06-05 16:16:13 +0000
Subject: Added missing points of interest in the St. Louis area

Commit:  f359c7973d3ca85a717559d6fbab070bf15b2e9e
Author:  admin <admin@demo.com>
Date:    (15 hours ago) 2017-06-04 22:05:15 -0300
Subject: Initial import of Missouri roads from 2015-01 osm dump

Commit:  5fa95e5ccb49311679d388b59339fb396bc5a22f
Author:  admin <admin@demo.com>
Date:    (15 hours ago) 2017-06-04 22:02:49 -0300
Subject: Initial import of Missouri points of interest from 2015-01 osm dump

Commit:  c0591e2e2e762018787633c7597dafb9b5d532b8
Author:  admin <admin@demo.com>
Date:    (15 hours ago) 2017-06-04 21:59:08 -0300
Subject: Initial import of Missouri buildings from 2015-01 osm dump

hannah$ gig branch fix_fieldwork
Created branch refs/heads/fix_fieldwork
hannah$ gig checkout fix_fieldwork
Switched to branch 'fix_fieldwork'
hannah$ gig branch
  fieldwork
* fix_fieldwork
  master
```

* Perform the fix and upload to `integration`

```
hannah$ gig revert HEAD
hannah$ gig diff HEAD~
00000000... 39b99ec7... 00000000... 76fc6185...   A  points/fid--72ed05ec_15c790ad9d6_-8000
the_geom	POINT (-90.18477644191731 38.624693351094884)
osm_id	
timestamp	
name	Gateway Arc
type	

hannah$ gig ql "update points set name = 'Gateway Arch' where name = 'Gateway Arc'"
Updated 1 features
hannah$ gig add
Counting unstaged elements...2
Staging changes...
100%
1 features and 1 trees staged for commit
0 features and 0 trees not staged for commit
hannah$ gig st
# On branch fix_fieldwork
# Changes to be committed:
#   (use "geogig reset HEAD <path/to/fid>..." to unstage)
#
#      modified  points
#      modified  points/fid--72ed05ec_15c790ad9d6_-8000
# 2 total.
#
hannah$ gig commit --amend -m "Fix typo on Gateway Arch's name"
100%
[65adfda7f9a487667bc88dbfd60dced12e3a749d] Fix typo on Gateway Arch's name
Committed, counting objects...1 features added, 0 changed, 0 deleted.
hannah$ gig log --oneline
hannah$ gig diff HEAD~
00000000... 39b99ec7... 00000000... 7177a4dc...   A  points/fid--72ed05ec_15c790ad9d6_-8000
the_geom	POINT (-90.18477644191731 38.624693351094884)
osm_id	
timestamp	
name	Gateway Arch
type	

hannah$ gig push origin fix_fieldwork

Uploading objects to fix_fieldwork
```
* Accept the fix

```
gabe$ gig branch
  fieldwork
  fix_fieldwork
* master
gabe$ gig checkout fix_fieldwork
Switched to branch 'fix_fieldwork'
gabe$ gig log --oneline --abbrev-commit
65adfda7 Fix typo on Gateway Arch's name
da26052f Revert commit abe21369b813f2fce3093760e52f5e627005daf0, there's a typo in the Arch's name
e8e78219 Update type of Boundless academy
abe21369 Added missing points of interest in the St. Louis area
f359c797 Initial import of Missouri roads from 2015-01 osm dump
5fa95e5c Initial import of Missouri points of interest from 2015-01 osm dump
c0591e2e Initial import of Missouri buildings from 2015-01 osm dump
gabe$ 
gabe$ gig diff master
00000000... 39b99ec7... 00000000... 7177a4dc...   A  points/fid--72ed05ec_15c790ad9d6_-8000
the_geom	POINT (-90.18477644191731 38.624693351094884)
osm_id	
timestamp	
name	Gateway Arch
type	

gabe$ gig checkout master
Switched to branch 'master'
gabe$ gig merge fix_fieldwork

Checking for possible conflicts...
Merging commit 65adfda7f9a487667bc88dbfd60dced12e3a749d
Conflicts: 0, merged: 0, unconflicted: 2
[65adfda7f9a487667bc88dbfd60dced12e3a749d] Fix typo on Gateway Arch's name
Committed, counting objects...1 features added, 0 changed, 0 deleted.
gabe$ gig branch
  fieldwork
  fix_fieldwork
* master
gabe$ gig push origin master

Uploading objects to master
```

## Dave's office work

* As dave:

```
dave$ gig branch
* master
dave$ gig branch 1601-osm-import
gig cCreated branch refs/heads/1601-osm-import
dave$ gig checkout 1601-osm-import 
Switched to branch '1601-osm-import'
dave$ gig branch
* 1601-osm-import
  master
dave$ gig shp import --add --fid-attrib osm_id ../missouri-1601/points.shp 
Importing from shapefile ../missouri-1601/points.shp
Importing points           (1/1)... 
40,872
52,899 features inserted in 1.146 s

Building final tree points...

55,342 features tree built in 170.9 ms
../missouri-1601/points.shp imported successfully.
dave$ gig add
Counting unstaged elements...12826
Staging changes...
12,825 features and 1 trees staged for commit
0 features and 0 trees not staged for commit
dave$ gig commit -m "Import of 2015 to 2016 Missouri points of interest from OSM"
100%
[fe80b4c16ffc8e9b6287b23c8ab3623292295ca2] Import of 2015 to 2016 Missouri points of interest from OSM
Committed, counting objects...11224 features added, 1601 changed, 0 deleted.
dave$ gig diff master --count
Trees: added 0, changed 1, removed 0
Features: added 11,224, changed 1,601, removed 0, total: 12,825
dave$ 
dave$ 
dave$ gig push origin 1601-osm-import

Uploading objects to 1601-osm-import

```

## Merge Dave's bulk import

* As Gabe:

```
gabe$ gig br
  1601-osm-import
  fieldwork
  fix_fieldwork
* master
gabe$ gig merge 1601-osm-import

Checking for possible conflicts...
Possible conflicts. Creating intermediate merge status...

Saving 1 merged features...
1 features inserted in 34.52 ms
Building final tree points...
44,119 features tree built in 33.05 ms
Staging 12,823 unconflicted and 1 merged differences...
Building final tree points
Done.

Saving 1 conflicts...
CONFLICT: Merge conflict in points/358035969
Automatic merge failed. Fix conflicts and then commit the result.
```

* See the conflict and where does it come from:

```
gabe$ gig status --limit 2
# On branch master
# Changes to be committed:
#   (use "geogig reset HEAD <path/to/fid>..." to unstage)
#
#      modified  points
#      added  points/3409446295
# 12,825 total.
#
# Unmerged paths:
#   (use "geogig add/rm <path/to/fid>..." as appropriate to mark resolution
#
#      unmerged  points/358035969
# 1 total.
gabe$ 
gabe$ gig conflicts
points/358035969

Ancestor	8f129ba6becb1ee1fae342df829f9f17fab467d0
id	8f129ba6becb1ee1fae342df829f9f17fab467d0
FEATURE
POINT	POINT (-92.4543481 39.4186465)
STRING	358035969
STRING	2014-11-06T12:49:15Z
STRING	Moberly Area Community College
STRING	school


Ours	2b780dac022df4e42beebbe9b56c2d4e58121432
id	2b780dac022df4e42beebbe9b56c2d4e58121432
FEATURE
POINT	POINT (-92.4543481 39.4186465)
STRING	358035969
STRING	2014-11-06T12:49:15Z
STRING	Moberly Area Community College
STRING	academy


Theirs	d50fc852883f5a993e44edd5ffd6e448afbdbf55
id	d50fc852883f5a993e44edd5ffd6e448afbdbf55
FEATURE
POINT	POINT (-92.4533641 39.4185997)
STRING	358035969
STRING	2015-12-20T21:47:41Z
STRING	Moberly Area Community College
STRING	college

gabe$ gig conflicts --diff
---points/358035969---
Ours
type: school -> academy

Theirs
the_geom: Point [-92.4543481,39.4186465] (-92.4533641,39.4185997) / 
timestamp: 2014-11-06T12:49:15Z -> 2015-12-20T21:47:41Z
type: school -> college

```
* Select the side of the conflict to keep
We're keeping Hannah's change since it's newer than the 2016 osm import

```
gabe$ gig checkout --ours --path  points/358035969
Objects in the working tree were updated to the specifed version.
gabe$ gig diff HEAD~ --path points/358035969
39b99ec7... 39b99ec7... 8f129ba6... 2b780dac...   M  points/358035969
type: school -> academy
gabe$ gig status --limit 2
# On branch master
# Changes to be committed:
#   (use "geogig reset HEAD <path/to/fid>..." to unstage)
#
#      modified  points
#      added  points/189889024
# 12,825 total.
#
# Unmerged paths:
#   (use "geogig add/rm <path/to/fid>..." as appropriate to mark resolution
#
#      unmerged  points/358035969
gabe$ gig add
Counting unstaged elements...0
Staging changes...
Done. 1 unmerged conflicts.
12,824 features and 1 trees staged for commit
0 features and 0 trees not staged for commit
gabe$ gig commit -m "Merge branch 1601-osm-import"
100%
[25e6f00cd1eb8d94e000dca9eae551071b1eeb63] Merge branch 1601-osm-import
Committed, counting objects...11224 features added, 1600 changed, 0 deleted.
```

* Upload to Gold repo

As Gabe:

```
gabe$ gig push origin master

Uploading objects to master
```

As admin:

```
gold$ gig log --oneline --abbrev-commit
25e6f00c Merge branch 1601-osm-import
fef2dac1 fix academy
fe80b4c1 Import of 2015 to 2016 Missouri points of interest from OSM
65adfda7 Fix typo on Gateway Arch's name
da26052f Revert commit abe21369b813f2fce3093760e52f5e627005daf0, there's a typo in the Arch's name
e8e78219 Update type of Boundless academy
abe21369 Added missing points of interest in the St. Louis area
f359c797 Initial import of Missouri roads from 2015-01 osm dump
5fa95e5c Initial import of Missouri points of interest from 2015-01 osm dump
c0591e2e Initial import of Missouri buildings from 2015-01 osm dump
```


