from xml.dom import minidom
import sys



# parse an xml file by name
routes = minidom.parse(sys.argv[1])
minX = float(sys.argv[2])
minY = float(sys.argv[3])
incr = float(sys.argv[4])
outputDir = sys.argv[5]
vehicles = routes.getElementsByTagName('vehicle')

def getXYFromStr(strPos):
    x,y = strPos.split("/")
    return float(x), float(y)

def getPositionPairsFromStr(strPosPair):
    strPosOld, strPosNew = strPosPair.split("to")
    oldX, oldY = getXYFromStr(strPosOld)
    newX, newY = getXYFromStr(strPosNew)
    return oldX, oldY, newX, newY

def getAdjustedPosition(val, start, incr):
    return start + val*incr

def createTrajectoryFile(locations, filename):
    fh = open(filename, 'w')
    for l in locations:
        line = str(l[0]) + "," + str(l[1]) + "\n"
        fh.write(line)
    fh.close()
 
for vehicle in vehicles:
    print(vehicle.attributes['id'].value)
    routes = vehicle.getElementsByTagName('route')
    routeNum = 0
    for route in routes:
        routeStr = route.attributes['edges'].value
        positions = routeStr.split(" ")
        
        locations = [] 
        for i in range(len(positions)):
            oldX, oldY, newX, newY = getPositionPairsFromStr(positions[i])
            locations.append((getAdjustedPosition(oldX, minX, incr), getAdjustedPosition(oldY, minY, incr)))
                
        createTrajectoryFile(locations, outputDir + "/" + vehicle.attributes['id'].value+ "_" + str(routeNum) +  ".txt")
        routeNum += 1

