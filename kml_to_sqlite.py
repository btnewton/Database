# Brandt Newton
# Barre Forest Guide Database Builder
# 10/27/2014

# NOTE: There does seem to be some loss in precission between processing
# 			coordinates and inserting them into the database. The 15th
# 			decimal place is lost. This is NOT an issue because such a
# 			high level of precission is less than a micron and is
# 			probably innacurate. Smart phone GPS hardware accuracy is
# 			limited to a magnitude of meters. The 5th decimal place
# 			is accurate to 1.1 meters and that is sufficient.

from xml.dom import minidom
import sqlite3

# Filenames & paths
kml_directory = ''
kml_file = 'MillstoneTrails.kml'

db_directory = ''
db_file = 'bfg.sqlite'

# KML
kmldoc = minidom.parse(kml_directory + kml_file)

# SQLite Database connection
connection = sqlite3.connect(db_directory + db_file)

# Global Variables
cursor = connection.cursor()


# Main() calls all functions and allows unwanted functions to be commented out.
def main():

	# Drop & create tables
	drop_tables()
	create_tables()

	add_placemarkers()

	add_poi_types()

	add_points_of_interest()

	# Close connection
	cursor.close()
	print("Program Complete.")


# Add_placemarkers() proesses the information for each placemarker in the KML file and 
# inserts it into the database. Processing and table insertion happens every
# iteration of the primary for loop.
def add_placemarkers():

	# Used to count the number of unique tags processed.
	tag_counter = 0

	# Used to count number of trails added
	placemark_counter = 0

	# Begin processing placemarkers in KML file.

	kml = kmldoc.getElementsByTagName('kml')[0]
	document = kml.getElementsByTagName('Document')[0]
	placemarks = document.getElementsByTagName('Placemark')

	print("Processing placemarkers...")
	for placemark in placemarks:

		# Get names, tags & distance
		extended_data = placemark.getElementsByTagName('ExtendedData')[0]
		data = extended_data.getElementsByTagName('SchemaData')[0].childNodes

		summer_tag = data[1].firstChild.nodeValue 			# Summer tag
		winter_tag = data[3].firstChild.nodeValue 			# Winter tag
		summer_name = data[7].firstChild.nodeValue 			# Summer name
		winter_name = data[9].firstChild.nodeValue 			# Winter name
		distance = float(data[21].firstChild.nodeValue) 	# Distance (meters)

		# Get coordinates
		lineString = placemark.getElementsByTagName('LineString')[0]
		coordinate_string = lineString.getElementsByTagName('coordinates')[0].firstChild.nodeValue

		coordinates = coordinate_string.split(',')

		coordinate_counter = 0

		for coordinate in coordinates:
			if coordinate[:2] =='0 ':
				coordinate = coordinate[2:]

			try:
				float_coordinate = float(coordinate)

			except ValueError:
				# Coordinate is some String
				print("Invalid coordinate:", coordinate, ". Placemark:", placemark_counter, ".")

			else:
				# Coordinate is valid
				if float_coordinate != 0:
					coordinates[coordinate_counter] = float_coordinate
					coordinate_counter += 1

		# Insert trails information
		url = ""
		trail_values = [placemark_counter, summer_name, winter_name, distance, url]
		cursor.execute("""INSERT INTO trails VALUES(?, ?, ?, ?, ?)""", trail_values)

		# Insert trail tags & tag-trail relationship
		tag_counter = add_tag(summer_tag, placemark_counter, tag_counter)

		if summer_tag != winter_tag:
			tag_counter = add_tag(winter_tag, placemark_counter, tag_counter)

		# Insert trail coordinates
		for i in range(0, int(len(coordinates)/2)):
			longitude = coordinates[i*2]
			lattitude = coordinates[i*2 +1]

			coordinate_values = [placemark_counter, lattitude, longitude]

			cursor.execute("""INSERT INTO coordinates VALUES(?, ?, ?)""", coordinate_values)

		connection.commit()

		placemark_counter += 1

	print("Done.")

# Checks to see if tag has already been stored. If this is a new tag it is stored.
# The trail-tag relationship is then added. The tag_counter is returned in case
# it was updated.
def add_tag(tag, trail_id, tag_counter):
	
	cursor.execute("""SELECT id FROM trail_tags WHERE tag LIKE ? """, (tag,))
	selected_tag = cursor.fetchone()


	if selected_tag == None:
		tag_id = tag_counter

		# Store new tag
		tag_values = [tag_id, tag]
		cursor.execute("""INSERT INTO trail_tags VALUES(?, ?)""", tag_values)

		tag_counter += 1

	else:
		# Get tag_id of previously stored tag
		tag_id = int(selected_tag[0])

	# Add tag-trail relationship
	trail_tag_values = [trail_id, tag_id]
	cursor.execute("""INSERT INTO trail_tag_ids VALUES(?, ?)""", trail_tag_values)

	# Update global non-local tag_counter
	return tag_counter


def drop_tables():
	print("Dropping tables...")
	cursor.execute("""DROP TABLE IF EXISTS trails""")
	cursor.execute("""DROP TABLE IF EXISTS trail_tag_ids""")
	cursor.execute("""DROP TABLE IF EXISTS trail_tags""")
	cursor.execute("""DROP TABLE IF EXISTS coordinates""")
	cursor.execute("""DROP TABLE IF EXISTS points_of_interest""")
	cursor.execute("""DROP TABLE IF EXISTS poi_types""")
	print("Done.")

def create_tables():
	print("Creating tables...")

	# The trails table is used to store each unique trail or sub trail
	cursor.execute("""CREATE TABLE trails (
					id				INTEGER PRIMARY KEY,
					name_summer		TEXT,
					name_winter		TEXT,
					distance		REAL,
					url				TEXT

					)""")

	# Trail-tag tag many-to-many relationship table. This allows for
	# trails to have any number of tags.
	cursor.execute("""CREATE TABLE trail_tag_ids (
					trail_id 	INTEGER,
					tag_id 		INTEGER,
					FOREIGN KEY(trail_id) REFERENCES trails(id),
					FOREIGN KEY(tag_id) REFERENCES trail_tags(id)

					)""")

	# The trail_tags table lists all unique trail tags and is referenced
	# by the the trail_tag_ids table.
	cursor.execute("""CREATE TABLE trail_tags (
					id 		INTEGER	PRIMARY KEY,
	    			tag 	TEXT
					
					)""")

	# The coordinates table stores all coordinates for the trails.
	cursor.execute("""CREATE TABLE coordinates (
					trail_id 	INTEGER,
					lattitude 	REAL,
					longitude	REAL,
					FOREIGN KEY(trail_id) REFERENCES trails(id)

					)""")

	cursor.execute("""CREATE TABLE poi_types (
					name 		TEXT

					)""")

	cursor.execute("""CREATE TABLE points_of_interest (
					name 		TEXT,
					type		INTEGER,
					lattitude	REAL,
					longitude	REAL,
					url			TEXT,
					FOREIGN KEY(type) REFERENCES poi_types(rowid)

					)""")

	connection.commit()
	print("Done.")



def add_poi_types():
	print("Adding POI types")
	poi_types = []

	#Add POI types here. Note the corresponding rowid!
	poi_types.append('Parking Lot') 	#1 (rowid) *Parking lots should only be listed if they are for Forest users.
	poi_types.append('Overlook')		#2
	poi_types.append('Store')			#3
	poi_types.append('Historic Site')	#4

	for i in range(0, len(poi_types)):
		cursor.execute("""INSERT INTO poi_types VALUES(?)""", (poi_types[i],))

	connection.commit()
	print("Done.")

def add_points_of_interest():
	print("Adding POI")

	# Creates a list containing 5 lists initialized to 0
	poi_values = []
	
	poi_values.append(["MTA Shop", 3, 44.159882, -72.470772, None])
	poi_values.append(["Lawson's Store", 3, 44.159483, -72.470880, None])
	poi_values.append(["South View", 2, 44.136259, -72.491225, None])
	poi_values.append(["Grand Lookout", 2, 44.161278, -72.476250, None])
	poi_values.append(["Brook St. Parking Lot", 1, 44.157065, -72.469682, None])
	poi_values.append(["Little John Parking Lot", 1, 44.155471, -72.462611, None])
	poi_values.append(["Littlejohn & Milne Quarry", 4, 44.153975, -72.460692, None])
	poi_values.append(["The Couture/Wheeler Farm", 4, 44.159102, -72.467794, None])

	for i in range(0, len(poi_values)):
		cursor.execute("""INSERT INTO points_of_interest VALUES(?, ?, ?, ?, ?)""", poi_values[i])

	connection.commit()

	print("Done.")

main()
