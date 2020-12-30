import json
import os
import shapefile
import sys
import tqdm
import requests
import zipfile


mydir = os.path.dirname(os.path.abspath(__file__))
srcdatapath = os.path.join(mydir, "source_data")
outdatapath = os.path.join(mydir, "data")

def join_to_root(root, *args):
    new_args = []
    for each_arg in args:
        new_args.extend(each_arg.split("/"))
    new_args = [a for a in new_args if len(a) > 0]
    return os.path.join(root, *args)

def src(*args):
    return join_to_root(srcdatapath, *args)

def out(*args):
    return join_to_root(outdatapath, *args)

def download(url: str, fpath: str):
    r = requests.get(url, stream=True)
    if r.ok:
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        with open(fpath, "wb") as f:
            for chunk in tqdm.tqdm(iterable=r.iter_content(1024), desc=f"Downloading {os.path.basename(fpath)}"):
                f.write(chunk)
    return r.ok


def unzip(zippath: str, outpath: str):
    with zipfile.ZipFile(zippath, "r") as zf:
        print(f"Extracting {os.path.basename(zippath)} to {os.path.basename(outpath)}")
        zf.extractall(outpath)

def download_and_extract(url: str, fpath: str, outdir: str):
    if not os.path.isfile(fpath):
        worked = download(url, fpath)
    else:
        print(f"Skipping download for: {os.path.basename(fpath)}")
        worked = True
    if worked:
        unzip(fpath, outdir)
        #os.remove(fpath)
    return worked

def cat_files(infile: str, outfile:str):
    with open(outfile, "a") as outf:
        with open(infile, "r") as inf:
            outf.write(inf.read())

def download_all():
    # DOwnload zipcode data
    download_and_extract("http://download.geonames.org/export/zip/US.zip", src("US.zip"), src())
    download_and_extract("http://download.geonames.org/export/zip/PR.zip", src("PR.zip"), src())
    download_and_extract("http://download.geonames.org/export/zip/GU.zip", src("GU.zip"), src())
    download_and_extract("http://download.geonames.org/export/zip/VI.zip", src("VI.zip"), src())
    download_and_extract("http://download.geonames.org/export/zip/AS.zip", src("AS.zip"), src())
    
    # Move territories into main US file
    cat_files(src("PR.txt"), src("US.txt"))
    cat_files(src("GU.txt"), src("US.txt"))
    cat_files(src("VI.txt"), src("US.txt"))
    cat_files(src("AS.txt"), src("US.txt"))

    # Download shape data
    download_and_extract("http://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_zcta510_500k.zip", src("census.zip"), src())



class GeonamesEntry(object):

    def __init__(self, delimited_string):
        parts = delimited_string.split('\t')
        self.country_code = parts[0]
        self.postal_code = parts[1]
        self.name = parts[2]
        self.state = parts[4]
        self.county_code = parts[6]
        self.lat = float(parts[9])
        self.lng = float(parts[10])

        # Special case - if these are records for US territories,
        # we need to change the state. The source data has the
        # admin1 code as something other than what we expect
        if self.country_code in ('PR', 'VI', 'AS', 'GU'):
            self.state = self.country_code

    def __repr__(self):
        return self.postal_code

class ZipCodeRecord(object):

    def __init__(self):
        self.postal_code = None
        self.county_code = None
        self.state = None
        self.city = None
        self.shape = None
        self.latitude = None
        self.longitude = None

    def __repr__(self):
        return self.postal_code

    def to_geojson(self):
        return {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [
                            self.longitude,
                            self.latitude
                        ]
                    },
                    'properties': {}
                },
                {
                    'type': 'Feature',
                    'geometry': self.shape.__geo_interface__,
                    'properties': {
                        'postal-code': self.postal_code,
                        'county-code': self.county_code,
                        'state': self.state,
                        'city': self.city,
                    }
                }
            ]
        }

def process():
    # Get all the entries from Geonames
    with open(src('US.txt'), 'r') as f:
        entries = [line.strip() for line in f.readlines()]

    # Index the entries in a dictionary by key
    entries_by_zipcode = {}
    for line in entries:
        entry = GeonamesEntry(line)
        entries_by_zipcode[entry.postal_code] = entry

    # Get all the records from the shapefile
    sf = shapefile.Reader(src("cb_2019_us_zcta510_500k.shp"))

    # Get references to the shapes and records inside the shapefile
    shapes = sf.shapes()
    records = sf.records()

    # Store a list of all the entries
    parsed_zcrecords = []

    # Iterate over all the data in the shapefiles
    for index in range(0, len(shapes)):

        current_record = records[index]
        current_shape = shapes[index]

        zcrecord = ZipCodeRecord()
        zcrecord.postal_code = current_record[2]
        zcrecord.shape = current_shape

        # Find the corresponding entry in the indexed zip code data
        try:
            entry = entries_by_zipcode[zcrecord.postal_code]
        except KeyError:
            print("Could not find geonames entry for zip code %s. Skipping." % zcrecord.postal_code)
            continue

        zcrecord.county_code = entry.county_code
        zcrecord.state = entry.state
        zcrecord.city = entry.name
        zcrecord.latitude = entry.lat
        zcrecord.longitude = entry.lng

        parsed_zcrecords.append(zcrecord)

    with tqdm.tqdm(desc="Writing output files", total=len(parsed_zcrecords)) as progress:

        # Write the entries out to disk
        for index, parsed_zcrecord in enumerate(parsed_zcrecords):
            if len(parsed_zcrecord.postal_code) == 0:
                print(f"BAD RECORD: Skipping. (State: [{parsed_zcrecord.state}]) (Postal Code: [{parsed_zcrecord.postal_code}])")
                progress.update(1)
                continue

            if len(parsed_zcrecord.state) == 0:
                print(f"BAD RECORD: Using state ZZ instead. (State: [{parsed_zcrecord.state}]) (Postal Code: [{parsed_zcrecord.postal_code}])")
                state_to_use = "ZZ"
            else:
                state_to_use = parsed_zcrecord.state

            filename = out('%s/%s.geojson' % (state_to_use, parsed_zcrecord.postal_code))
            dirname = os.path.dirname(filename)
            # See if the output data directory has a subdirectory for this state
            if not os.path.exists(dirname):
                os.makedirs(dirname, exist_ok=True)

            if not os.path.isdir(dirname):
                print(f"Expected dir, got file: {dirname}")
            else:
                #print('Writing file %d of %d: %s' % (index, len(parsed_zcrecords), filename))
                with open(filename, 'w') as f:
                    f.write(json.dumps(parsed_zcrecord.to_geojson(), indent=4))
            progress.update(1)


if __name__ == '__main__':
    download_all()
    print("Output directory:")
    print(outdatapath)
    print("Source directory:")
    print(srcdatapath)
    process()
    