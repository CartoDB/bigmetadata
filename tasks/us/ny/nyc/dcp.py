from tasks.meta import OBSColumn, GEOM_REF, current_session
from tasks.util import (Shp2TempTableTask, DownloadUnzipTask, shell, TableTask,
                        ColumnsTask,MetaWrapper)
from tasks.poi import POIColumns
from tasks.us.ny.nyc.columns import NYCColumns
from tasks.us.ny.nyc.tags import NYCTags

from luigi import Parameter

from collections import OrderedDict
import os

class DownloadUnzipMapPLUTO(DownloadUnzipTask):

    borough = Parameter()
    release = Parameter()

    URL = 'http://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/{borough}_mappluto_{release}.zip'

    def download(self):

        shell('wget -O {output}.zip {url}'.format(
            output=self.output().path,
            url=self.URL.format(borough=self.borough, release=self.release)
        ))


class MapPLUTOTmpTable(Shp2TempTableTask):

    borough = Parameter()
    release = Parameter()

    def requires(self):
        return DownloadUnzipMapPLUTO(borough=self.borough, release=self.release)

    def input_shp(self):
        return os.path.join(self.input().path, '{}MapPLUTO.shp'.format(self.borough.upper()))


class MapPLUTOColumns(ColumnsTask):

    def requires(self):
        return {
            'tags': NYCTags(),
        }

    def version(self):
        return 5

    def columns(self):
        input_ = self.input()
        cols = OrderedDict([
            ("zonedist1", OBSColumn(
                type="Text",
                name='Zoning District 1',
                weight=1,
                description='''The zoning district classification of the tax
                lot. If the tax lot is divided by a zoning boundary line,
                ZoneDist1 represents the primary zoning district classification
                occupying the greates t percentage of the tax lot's area.
                Properties under the jurisdiction of NYC Department of Parks
                and Recreation are coded PARK. Properties under the
                jurisdiction of NYS Office of Parks, Recreation, and Historic
                Preservation are coded PARKNY.  DROP LOT is a designation that
                City Planning devised to identify tax lots that no longer exist
                in the DCP version of the Digital Tax Map but have not yet been
                removed from the Department of Finance RPAD File. RPAD retains
                tax lots that have been dropped, due to merger, reapportionment
                or conversion to condominium, until the end of the City's
                Fiscal Year. To avoid confusion DROP LOT was created to
                identify these lots.''')),
            ("zonedist2", OBSColumn(
                type="Text",
                name='Zoning District 2',
                weight=1,
                description='''If the tax lot is divided by zoning boundary
                lines, ZoneDist2 represents the primary zoning classification
                occupying the second greatest percentage of the tax lot's area.
                If the tax lot is not divided by a zoning boundary line, the
                field is blank.''')),
            ("zonedist3", OBSColumn(
                type="Text",
                weight=1,
                name='Zoning District 3',
                description='''If the tax lot is divided by zoning boundary
                lines, ZoneDist3 represents the primary zoning classification
                occupying the third greatest percentage of the tax lot's area.
                If the tax lot is not divided by a zoning boundary line, the
                field is blank.''')),
            ("zonedist4", OBSColumn(
                type="Text",
                weight=1,
                name='Zoning District 4',
                description='''If the tax lot is divided by zoning boundary
                lines, ZoneDist4 represents the primary zoning classification
                occupying the fourth greatest percentage of the tax lot's area.
                If the tax lot is not divided by a zoning boundary line, the
                field is blank.''')),
            ("overlay1", OBSColumn(
                type="Text",
                weight=1,
                name='Primary Commercial overlay',
                description='''The commercial overlay assigned to the tax lot.'''
            )),
            ("overlay2", OBSColumn(
                type="Text",
                weight=1,
                name='Secondary Commercial overlay',
                description="""A commercial overlay associated with the tax lot. """,
            )),
            ("spdist1", OBSColumn(
                type="text",
                weight=1,
                name="Primary special purpose district",
                description="The special purpose district assigned to the tax lot.")),
            ("spdist2", OBSColumn(
                type="text",
                weight=1,
                name="Secondary special purpose district",
                description="The special purpose district assigned to the tax lot.")),
            ("spdist3", OBSColumn(
                type="text",
                weight=1,
                name="Third special purpose district",
                description="The special purpose district assigned to the tax lot.")),
            ("ltdheight", OBSColumn(
                type="text",
                weight=1,
                name="Limited Height district",
                description="")),
            ("splitzone", OBSColumn(
                type="text",
                weight=1,
                name="Is the tax lot split by a zoning boundary",
                description="""A code indicating whether the tax lot is split by
                one or more zoning boundary lines.""")),
            ("bldgclass", OBSColumn(
                type="text",
                weight=1,
                name="Building class",
                description="""A code describing the major use of structures on
                the tax lot.""",
                extra={
                    "categories": {
                        'A0': '1 family: Cape Cod',
                        'A1': '1 family: Two Stories Detached (Small or'
                              'Moderate Size, With or Without Attic)',
                        'A2': '1 family: One Story (Permanent Living Quarters)',
                        'A3': '1 family: Large Suburban Residence',
                        'A4': '1 family: City Residence',
                        'A5': '1 family: Attached or Semi-Detached',
                        'A6': '1 family: Summer Cottages',
                        'A7': '1 family: Mansion Type or Town House',
                        'A8': '1 family: Bungalow Colony/Land Coop Owned',
                        'A9': '1 family: Miscellaneous',
                        'B1': '2 family: Brick',
                        'B2': '2 family: Frame',
                        'B3': '2 family: Converted From One Family',
                        'B9': '2 family: Miscellaneous',
                        'C0': 'Walk-up apt: Three Families',
                        'C1': 'Walk-up apt: Over Six Families Without Stores',
                        'C2': 'Walk-up apt: Five to Six Families',
                        'C3': 'Walk-up apt: Four Families',
                        'C4': 'Walk-up apt: Old Law T enements',
                        'C5': 'Walk-up apt: Converted Dwelling or Rooming House',
                        'C6': 'Walk-up apt: Cooperative',
                        'C7': 'Walk-up apt: Over Six Families With Stores',
                        'C8': 'Walk-up apt: Co-Op Conversion From Loft/Warehouse',
                        'C9': 'Walk-up apt: Garden Apartments',
                        'CM': 'Walk-up apt: Mobile Homes/Trailer Parks',
                        'D0': 'Elevator apt: Co-op Conversion from Loft/Warehouse',
                        'D1': 'Elevator apt: Semi-fireproof (Without Stores)',
                        'D2': 'Elevator apt: Artists in Residence',
                        'D3': 'Elevator apt: Fireproof (Without Stores)',
                        'D4': 'Elevator apt: Cooperatives (Other Than Condominiums)',
                        'D5': 'Elevator apt: Converted',
                        'D6': 'Elevator apt: Fireproof With Stores',
                        'D7': 'Elevator apt: Semi-Fireproof With Stores',
                        'D8': 'Elevator apt: Luxury Type',
                        'D9': 'Elevator apt: Miscellaneous',
                        'E1': 'Warehouse: Fireproof',
                        'E2': "Warehouse: Contractor's Warehouse",
                        'E3': 'Warehouse: Semi-Fireproof',
                        'E4': 'Warehouse: Frame, Metal',
                        'E7': 'Warehouse: Warehouse, Self Storage',
                        'E9': 'Warehouse: Miscellaneous',
                        'F1': 'Industrial: Heavy Manufacturing - Fireproof',
                        'F2': 'Industrial: Special Construction - Fireproof',
                        'F4': 'Industrial: Semi-Fireproof',
                        'F5': 'Industrial: Light Manufacturing',
                        'F8': 'Industrial: Tank Farms',
                        'F9': 'Industrial: Miscellaneous',
                        'G0': 'Garage/gas: Residential Tax Class 1 Garage',
                        'G1': 'Garage/gas: All Parking Garages',
                        'G2': 'Garage/gas: Auto Body/Collision or Auto Repair',
                        'G3': 'Garage/gas: Gas Station with Retail Store',
                        'G4': 'Garage/gas: Gas Station with Service/Auto Repair',
                        'G5': 'Garage/gas: Gas Station only with/without Small Kiosk',
                        'G6': 'Garage/gas: Licensed Parking Lot',
                        'G7': 'Garage/gas: Unlicensed Parking Lot',
                        'G8': 'Garage/gas: Car Sales/Rental with Showroom',
                        'G9': 'Garage/gas: Miscellaneous Garage or Gas Station',
                        'GU': 'Garage/gas: Car Sales/Rental without Showroom',
                        'GW': 'Garage/gas: Car Wash or Lubritorium Facility',
                        'H1': 'Hotel: Luxury Type',
                        'H2': 'Hotel: Full Service Hotel',
                        'H3': 'Hotel: Limited Service - Many Affiliated with National Chain',
                        'H4': 'Hotel: Motels',
                        'H5': 'Hotel: Private Club, Luxury Type',
                        'H6': 'Hotel: Apartment Hotels',
                        'H7': 'Hotel: Apartment Hotels-Co-op Owned',
                        'H8': 'Hotel: Dormitories',
                        'H9': 'Hotel: Miscellaneous',
                        'HB': 'Hotel: Boutique 10-100 Rooms, with Luxury '
                              'Facilities, Themed, Stylish, with Full Service '
                              'Accommodations',
                        'HH': 'Hotel: Hostel-Bed Rental in Dorm Like Setting '
                              'with Shared Rooms & Bathrooms',
                        'HR': 'Hotel: SRO- 1 or 2 People Housed in Individual '
                              'Rooms in Multiple Dwelling Affordable Housing',
                        'HS': 'Hotel: Extended Stay/Suite Amenities Similar to '
                              'Apt., Typically Charge Weekly Rates & Less '
                              'Expensive than Full Service Hotel',
                        'I1': 'Health: Hospitals, Sanitariums, Mental Institutions',
                        'I2': 'Health: Infirmary',
                        'I3': 'Health: Dispensary',
                        'I4': 'Health: Staff Facilities',
                        'I5': 'Health: Health Center, Child Center, Clinic',
                        'I6': 'Health: Nursing Home',
                        'I7': 'Health: Adult Care Facility',
                        'I9': 'Health: Miscellaneous',
                        'J1': 'Theatre: Art Type (Seating Capacity under 400 Seats)',
                        'J2': 'Theatre: Art Type (Seating Capacity Over 400 Seats)',
                        'J3': 'Theatre: Motion Picture Theatre with Balcony',
                        'J4': 'Theatre: Legitimate Theatres (Theatre Sole Use of Building)',
                        'J5': 'Theatre: Theatre in Mixed Use Building',
                        'J6': 'Theatre: T.V. Studios',
                        'J7': 'Theatre: Off-Broadway Type',
                        'J8': 'Theatre: Multiplex Picture Theatre',
                        'J9': 'Theatre: Miscellaneous',
                        'K1': 'Store: One Story Retail Building',
                        'K2': 'Store: Multi-Story Retail Building',
                        'K3': 'Store: Multi-Story Department Store',
                        'K4': 'Store: Predominant Retail with Other Uses',
                        'K5': 'Store: Stand Alone Food Establishment',
                        'K6': 'Store: Shopping Centers With or Without Parking',
                        'K7': 'Store: Banking Facilities with or Without Parking',
                        'K8': 'Store: Big Box Retail Not Affixed & Standing On '
                              'Own Lot with Parking',
                        'K9': 'Store: Miscellaneous',
                        'L1': 'Loft: Over Eight Stores (Mid-Manhattan Type)',
                        'L2': 'Loft: Fireproof and Storage Type (Without Stores)',
                        'L3': 'Loft: Semi-Fireproof',
                        'L8': 'Loft: With Retail Stores Other Than Type 1',
                        'L9': 'Loft: Miscellaneous',
                        'M1': 'Religious: Church, Synagogue, Chapel',
                        'M2': 'Religious: Mission House (non-Residential)',
                        'M3': 'Religious: Parsonage, Rectory',
                        'M4': 'Religious: Convents',
                        'M9': 'Religious: Miscellaneous',
                        'N1': 'Group home: Asylums',
                        'N2': 'Group home: Homes for Indigent Children, Aged, Homeless',
                        'N3': 'Group home: Orphanages',
                        'N4': 'Group home: Detention House For Wayward Girls',
                        'N9': 'Group home: Miscellaneous',
                        'O1': 'Office Only - 1 Story',
                        'O2': 'Office Only - 2-6 Stories',
                        'O3': 'Office Only - 7-19 Stories',
                        'O4': 'Office Only or Office with Comm - 20 Stories or More',
                        'O5': 'Office with Comm - 1 to 6 Stories',
                        'O6': 'Office with Comm - 7 to 19 Stories',
                        'O7': 'Office: Professional Buildings/Stand Alone Funeral Homes',
                        'O8': 'Office with Apartments Only (No Comm)',
                        'O9': 'Office: Miscellaneous and Old Style Bank Bldgs',
                        'P1': 'Cultural: Concert Halls',
                        'P2': 'Cultural: Lodge Rooms',
                        'P3': 'Cultural: YWCA, YMCA, YWHA, YMHA, PAL',
                        'P4': 'Cultural: Beach Club',
                        'P5': 'Cultural: Community Center',
                        'P6': 'Cultural: Amusement Place, Bathhouse, Boat House',
                        'P7': 'Cultural: Museum',
                        'P8': 'Cultural: Library',
                        'P9': 'Cultural: Miscellaneous',
                        'Q0': 'Recreation: Open Space',
                        'Q1': 'Recreation: Parks/Recreation Facilities',
                        'Q2': 'Recreation: Playground',
                        'Q3': 'Recreation: Outdoor Pool',
                        'Q4': 'Recreation: Beach',
                        'Q5': 'Recreation: Golf Course',
                        'Q6': 'Recreation: Stadium, Race Track, Baseball Field',
                        'Q7': 'Recreation: Tennis Court',
                        'Q8': 'Recreation: Marina, Yacht Club',
                        'Q9': 'Recreation: Miscellaneous',
                        'R0': 'Condo Billing Lot',
                        'R1': 'Condo: Residential Unit in 2-10 Unit Bldg',
                        'R2': 'Condo: Residential Unit in Walk-Up Bldg',
                        'R3': 'Condo: Residential Unit in 1-3 Story Bldg',
                        'R4': 'Condo: Residential Unit in Elevator Bldg',
                        'R5': 'Condo: Miscellaneous Commercial',
                        'R6': 'Condo: Residential Unit of 1-3 Unit Bldg-Orig Class 1',
                        'R7': 'Condo: Commercial Unit of 1-3 Units Bldg- Orig Class 1',
                        'R8': 'Condo: Commercial Unit of 2-10 Unit Bldg',
                        'R9': 'Condo: Co-op within a Condominium',
                        'RA': 'Condo: Cultural, Medical, Educational, etc.',
                        'RB': 'Condo: Office Space',
                        'RC': 'Condo: Commercial Building (Mixed Commercial '
                              'Condo Building Classification Codes)',
                        'RD': 'Condo: Residential Building (Mixed Residential '
                              'Condo Building Classification Codes)',
                        'RG': 'Condo: Indoor Parking',
                        'RH': 'Condo: Hotel/Boatel',
                        'RI': 'Condo: Mixed Warehouse/Factory/Industrial & Commercial',
                        'RK': 'Condo: Retail Space',
                        'RM': 'Condo: Mixed Residential & Commercial Building '
                              '(Mixed Residential & Commercial)',
                        'RP': 'Condo: Outdoor Parking',
                        'RR': 'Condo: Condominium Rentals',
                        'RS': 'Condo: Non-Business Storage Space',
                        'RT': 'Condo: Terraces/Gardens/Cabanas',
                        'RW': 'Condo: Warehouse/Factory/Industrial',
                        'RX': 'Condo: Mixed Residential, Commercial & Industrial',
                        'RZ': 'Condo: Mixed Residential & Warehouse',
                        'S0': 'Mixed residence: Primarily One Family with Two Stores or Offices',
                        'S1': 'Mixed residence: Primarily One Family with One Store or Office',
                        'S2': 'Mixed residence: Primarily Two Family with One Store or Office',
                        'S3': 'Mixed residence: Primarily Three Family with One Store or Office',
                        'S4': 'Mixed residence: Primarily Four Family with One Store or Office',
                        'S5': 'Mixed residence: Primarily Five to Six Family '
                              'with One Store or Office',
                        'S9': 'Mixed residence: Single or Multiple Dwelling with Stores or Offices',
                        'T1': 'Transportation: Airport, Air Field, Terminal',
                        'T2': 'Transportation: Pier, Dock, Bulkhead',
                        'T9': 'Transportation: Miscellaneous',
                        'U0': 'Utility: Company Land and Building',
                        'U1': 'Utility: Bridge, Tunnel, Highway',
                        'U2': 'Utility: Gas or Electric Utility',
                        'U3': 'Utility: Ceiling Railroad',
                        'U4': 'Utility: Telephone Utility',
                        'U5': 'Utility: Communications Facilities Other Than Telephone',
                        'U6': 'Utility: Railroad - Private Ownership',
                        'U7': 'Utility: Transportation - Public Ownership',
                        'U8': 'Utility: Revocable Consent',
                        'U9': 'Utility: Miscellaneous',
                        'V0': 'Vacant: Zoned Residential; Not Manhattan',
                        'V1': 'Vacant: Zoned Commercial or Manhattan Residential',
                        'V2': 'Vacant: Zoned Commercial Adjacent to Class 1 '
                              'Dwelling; Not Manhattan',
                        'V3': 'Vacant: Zoned Primarily Residential; Not Manhattan',
                        'V4': 'Vacant: Police or Fire Department',
                        'V5': 'Vacant: School Site or Yard',
                        'V6': 'Vacant: Library, Hospital or Museum',
                        'V7': 'Vacant: Port Authority of NY and NJ',
                        'V8': 'Vacant: New York State & U.S. Government',
                        'V9': 'Vacant: Miscellaneous',
                        'W1': 'Educational: Public Elementary, Junior or Senior High',
                        'W2': 'Educational: Parochial School, Yeshiva',
                        'W3': 'Educational: School or Academy',
                        'W4': 'Educational: Training School',
                        'W5': 'Educational: City University',
                        'W6': 'Educational: Other College and University',
                        'W7': 'Educational: Theological Seminary',
                        'W8': 'Educational: Other Private School',
                        'W9': 'Educational: Miscellaneous',
                        'Y1': 'Government: Fire Department',
                        'Y2': 'Government: Police Department',
                        'Y3': 'Government: Prison, Jail, House of Detention',
                        'Y4': 'Government: Military and Naval Installation',
                        'Y5': 'Government: Department of Real Estate',
                        'Y6': 'Government: Department of Sanitation',
                        'Y7': 'Government: Department of Ports and',
                        'Y8': 'Government: Department of Public Works',
                        'Y9': 'Government: Department of Environmental Protection',
                        'Z0': 'Misc: Tennis Court, Pool, Shed, etc.',
                        'Z1': 'Misc: Court House',
                        'Z2': 'Misc: Public Parking Area',
                        'Z3': 'Misc: Post Office',
                        'Z4': 'Misc: Foreign Government',
                        'Z5': 'Misc: United Nations',
                        'Z7': 'Misc: Easement',
                        'Z8': 'Misc: Cemetery',
                        'Z9': 'Misc: Other',
                    }
                })
            ),
            ("landuse", OBSColumn(
                type="text",
                weight=1,
                name="Land use category",
                description="""A code for the tax lot's land use category,
                modified for display of parks, New York City Department of
                Parks and Recreation properties and New York State Office of
                Parks, Recreation and H istoric Preservation properties in the
                appropriate category on land use maps.""",
                extra={
                    "categories": {
                        '01': 'One & Two Family Buildings',
                        '02': 'Multi - Family Walk - Up Buildings',
                        '03': 'Multi - Family Elevator Buildings',
                        '04': 'Mixed Residential & Commercial Buildings',
                        '05': 'Commercial & Office Buildings',
                        '06': 'Industrial & Manufacturing',
                        '07': 'Transportation & Utility',
                        '08': 'Public Facilities & Institutions',
                        '09': 'Open Space & Outdoor Recreation',
                        '10': 'Parking Facilities',
                        '11': 'Vacant Land',
                    }
                }
            )),
            ("easements", OBSColumn(
                type="text",
                weight=1,
                name="Easements",
                description="""The number of easements on the tax lot. If the
                number is zero, the tax lot has no easement""")),
            ("ownertype", OBSColumn(
                type="text",
                weight=1,
                name="Ownership Type",
                description="""A code indicating type of ownership for the tax lot.""")),
            ("ownername", OBSColumn(
                type="text",
                weight=1,
                name="Owner name",
                description="""The name of the owner of the tax lot (from RPAD).""")),
            ("lotarea", OBSColumn(
                type="Numeric",
                weight=1,
                name="Lot Area",
                description=""" """)),
            ("bldgarea", OBSColumn(
                type="Numeric",
                weight=1,
                name="Building Area",
                description=""" """)),
            ("comarea", OBSColumn(
                type="Numeric",
                weight=1,
                name="Commercial Area",
                description=""" """)),
            ("resarea", OBSColumn(
                type="Numeric",
                weight=1,
                name="Residential Area",
                description=""" """)),
            ("officearea", OBSColumn(
                type="Numeric",
                weight=1,
                name="Office Area",
                description=""" """)),
            ("retailarea", OBSColumn(
                type="Numeric",
                weight=1,
                name="Retail Area",
                description=""" """)
            ),
            ("garagearea", OBSColumn(
                type="Numeric",
                weight=1,
                name="Garage Area",
                description=""" """)
            ),
            ("strgearea", OBSColumn(
                type="Numeric",
                weight=1,
                name="Storage Area",
                description=""" """)
            ),
            ("factryarea", OBSColumn(
                type="Numeric",
                weight=1,
                name="Factory Area",
                description=""" """)
            ),
            ("otherarea", OBSColumn(
                type="Numeric",
                weight=1,
                name="Other Area",
                description=""" """)
            ),
            ("areasource", OBSColumn(
                type="Text",
                weight=1,
                name="Area source",
                description=""" """)
            ),
            ("numbldgs", OBSColumn(
                type="Numeric",
                weight=1,
                name="Number of buildings",
                description=""" """)
            ),
            ("numfloors", OBSColumn(
                type="Numeric",
                weight=1,
                name="Floors",
                description=""" """)
            ),
            ("unitsres", OBSColumn(
                type="Numeric",
                weight=1,
                name="Residential units",
                description=""" """)
            ),
            ("unitstotal", OBSColumn(
                type="Numeric",
                weight=1,
                name="Total units",
                description=""" """)
            ),
            ("lotfront", OBSColumn(
                type="Numeric",
                weight=1,
                name="Lot front",
                description=""" """)
            ),
            ("lotdepth", OBSColumn(
                type="Numeric",
                weight=1,
                name="Lot depth",
                description=""" """)
            ),
            ("bldgfront", OBSColumn(
                type="Numeric",
                weight=1,
                name="Building Front",
                description=""" """)
            ),
            ("bldgdepth", OBSColumn(
                type="Numeric",
                weight=1,
                name="Building depth",
                description=""" """)
            ),
            ("ext", OBSColumn(
                type="Text",
                weight=1,
                name="Has extension",
                description="""A code identifying whether there is an extension or free
                standing structure on the lot which is not the primary structure""")
            ),
            ("proxcode", OBSColumn(
                type="Text",
                weight=1,
                name="Proximity code",
                description="""The physical relationship of the building to neighboring
                buildings.""")
            ),
            ("irrlotcode", OBSColumn(
                type="Text",
                weight=1,
                name="Irregularly shaped code",
                description="""A code indicating whether th e tax lot is irregularly shaped""")
            ),
            ("lottype", OBSColumn(
                type="Text",
                weight=1,
                name="Lot type code",
                description="""A code indicating the location of the tax lot to
                another tax lot and/or the water.""")
            ),
            ("bsmtcode", OBSColumn(
                type="Text",
                weight=1,
                name="Basement code",
                description="""A code describing the basement type/grade.""")
            ),
            ("assessland", OBSColumn(
                type="Numeric",
                weight=1,
                name="Assessed land value",
                description="""The tentative assessed land value for the upcoming fiscal year""")
            ),
            ("assesstot", OBSColumn(
                type="Numeric",
                weight=1,
                name="Assessed total value",
                description="""The tentative assessed total value for the upcoming fiscal year """)
            ),
            ("exemptland", OBSColumn(
                type="Numeric",
                weight=1,
                name="Exempt land value",
                description=""" """)
            ),
            ("exempttot", OBSColumn(
                type="Numeric",
                weight=1,
                name="Exempt total value",
                description=""" """)
            ),
            ("yearbuilt", OBSColumn(
                type="Numeric",
                weight=1,
                name="Year built",
                description=""" """)
            ),
            ("yearalter1", OBSColumn(
                type="Numeric",
                weight=1,
                name="Year of most recent alteration",
                description=""" """)
            ),
            ("yearalter2", OBSColumn(
                type="Numeric",
                weight=1,
                name="Year of second most recent alteration",
                description=""" """)
            ),
            ("histdist", OBSColumn(
                type="Text",
                weight=1,
                name="Historical district",
                description=""" """)
            ),
            ("landmark", OBSColumn(
                type="Text",
                weight=1,
                name="Name of landmark",
                description=""" """)
            ),
            ("builtfar", OBSColumn(
                type="Numeric",
                weight=1,
                name="Built floor area ratio (FAR)",
                description=""" """)
            ),
            ("residfar", OBSColumn(
                type="Numeric",
                weight=1,
                name="Maximum allowable residential floor area ratio (FAR)",
                description=""" """)
            ),
            ("commfar", OBSColumn(
                type="Numeric",
                weight=1,
                name="Maximum allowable commercial floor area ratio (FAR)",
                description=""" """)
            ),
            ("facilfar", OBSColumn(
                type="Numeric",
                weight=1,
                name="Maximum allowable facilities floor area ratio (FAR)",
                description=""" """)
            ),
            ("condono", OBSColumn(
                type="Numeric",
                weight=1,
                name="Condo number",
                description=""" """)
            ),
            ("xcoord", OBSColumn(
                type="Numeric",
                name="X Coordinate",
                description=""" """)
            ),
            ("ycoord", OBSColumn(
                type="Numeric",
                name="Y coordinate",
                description=""" """)
            ),
            ("zonemap", OBSColumn(
                type="Text",
                weight=1,
                name="Zoning map number",
                description=""" """)
            ),
            ("zmcode", OBSColumn(
                type="Text",
                weight=1,
                name="Zoning map border code",
                description=""" """)
            ),
            ("sanborn", OBSColumn(
                type="Text",
                weight=1,
                name="Sanborn map number",
                description=""" """)
            ),
            ("taxmap", OBSColumn(
                type="Text",
                weight=1,
                name="Department of Finance tax map volume number",
                description=""" """)
            ),
            ("edesignum", OBSColumn(
                type="text",
                weight=1,
                name="E-Designation number",
                description="")),
            ("appbbl", OBSColumn(
                type="text",
                weight=1,
                name="Pre-apportionment BBL",
                description="""The originating Borough, Tax Block and Tax Lot
                from the apportionment prior to the merge, split or property's
                conversion to a condominium.  The Apportionment BBL is only
                available for mergers, splits and conversions since 1984""")),
            ("appdate", OBSColumn(
                type="text",
                weight=1,
                name="Apportionment date",
                description="The date of the Apportionment in the format MM/DD/YYYY.")),
            ("plutomapid", OBSColumn(
                type="Numeric",
                weight=1,
                name="PLUTO Map ID",
                description="")),
            ("version", OBSColumn(
                type="text",
                weight=1,
                name="MapPLUTO version number",
                description="")),
            ("mappluto_f", OBSColumn(
                type="text",
                weight=1,
                name="MapPLUTO Flag",
                description="""The Department of Finance's DTM handles
                condominium lots differently from many other MapPLUTO sources.
                The DTM Tax Lot Polygon feature class uses the base borough
                - block - lot (BBL) as the unique identifier of a parcel
                currently occupied by a condominium.  The Department of City
                Planning and some of the other data sources for MapPLUTO use
                the billing bbl for condominiums.  Therefore, in creating
                MapPLUTO from DTM, DCP has had to reassign the billing bbl as
                the primary key for condominium tax parcels. In most cases,
                there is one to one relationship between the DTM's base bbl and
                MapPLUTO's billing bbl. In some cases, further processing has
                been necessary.  In a very few cases, non - condominium tax
                lots have also been modified. All of these cases are identified
                in the MapPluto Flag field.  The data type for MapPLUTO flag is
                a number, each number represents how the base bbl is
                reassigned.""")),
            #("shape_leng", OBSColumn(
            #    type="NUmeric",
            #    name="",
            #    description="")),
            #("shape_area", OBSColumn(
            #    type="Numeric",
            #    name="",
            #    description="")),
        ])
        return cols

    def tags(self, input_, col_key, col):
        return input_['tags']['nyc']


class MapPLUTO(TableTask):

    release = Parameter()

    def version(self):
        return 2

    def requires(self):
        data = {}
        for borough in ('bx', 'bk', 'mn', 'qn', 'si'):
            data[borough] = MapPLUTOTmpTable(borough=borough, release=self.release)

        return {
            'data': data,
            'pluto_columns': MapPLUTOColumns(),
            'nyc_columns': NYCColumns(),
            'poi_columns': POIColumns(),
        }

    def columns(self):
        input_ = self.input()
        poi = input_['poi_columns']
        nyc = input_['nyc_columns']
        pluto = input_['pluto_columns']
        return OrderedDict([
            ("borough", nyc["borough"]),
            ("block", nyc["block"]),
            ("lot", nyc["lot"]),
            ("cd", nyc["cd"]),
            ("ct2010", nyc["ct2010"]),
            ("cb2010", nyc["cb2010"]),
            ("schooldist", nyc["schooldist"]),
            ("council", nyc["council"]),
            ("zipcode", poi["postal_code"]),
            ("firecomp", nyc["firecomp"]),
            ("policeprct", nyc["policeprct"]),
            ("healtharea", nyc["healtharea"]),
            ("sanitboro", nyc["sanitboro"]),
            ("sanitdistr", nyc["sanitdistr"]),
            ("sanitsub", nyc["sanitsub"]),
            ("address", poi["address"]),
            ("zonedist1", pluto["zonedist1"]),
            ("zonedist2", pluto["zonedist2"]),
            ("zonedist3", pluto["zonedist3"]),
            ("zonedist4", pluto["zonedist4"]),
            ("overlay1", pluto["overlay1"]),
            ("overlay2", pluto["overlay2"]),
            ("spdist1", pluto["spdist1"]),
            ("spdist2", pluto["spdist2"]),
            ("spdist3", pluto["spdist3"]),
            ("ltdheight", pluto["ltdheight"]),
            ("splitzone", pluto["splitzone"]),
            ("bldgclass", pluto["bldgclass"]),
            ("landuse", pluto["landuse"]),
            ("easements", pluto["easements"]),
            ("ownertype", pluto["ownertype"]),
            ("ownername", pluto["ownername"]),
            ("lotarea", pluto["lotarea"]),
            ("bldgarea", pluto["bldgarea"]),
            ("comarea", pluto["comarea"]),
            ("resarea", pluto["resarea"]),
            ("officearea", pluto["officearea"]),
            ("retailarea", pluto["retailarea"]),
            ("garagearea", pluto["garagearea"]),
            ("strgearea", pluto["strgearea"]),
            ("factryarea", pluto["factryarea"]),
            ("otherarea", pluto["otherarea"]),
            ("areasource", pluto["areasource"]),
            ("numbldgs", pluto["numbldgs"]),
            ("numfloors", pluto["numfloors"]),
            ("unitsres", pluto["unitsres"]),
            ("unitstotal", pluto["unitstotal"]),
            ("lotfront", pluto["lotfront"]),
            ("lotdepth", pluto["lotdepth"]),
            ("bldgfront", pluto["bldgfront"]),
            ("bldgdepth", pluto["bldgdepth"]),
            ("ext", pluto["ext"]),
            ("proxcode", pluto["proxcode"]),
            ("irrlotcode", pluto["irrlotcode"]),
            ("lottype", pluto["lottype"]),
            ("bsmtcode", pluto["bsmtcode"]),
            ("assessland", pluto["assessland"]),
            ("assesstot", pluto["assesstot"]),
            ("exemptland", pluto["exemptland"]),
            ("exempttot", pluto["exempttot"]),
            ("yearbuilt", pluto["yearbuilt"]),
            ("yearalter1", pluto["yearalter1"]),
            ("yearalter2", pluto["yearalter2"]),
            ("histdist", pluto["histdist"]),
            ("landmark", pluto["landmark"]),
            ("builtfar", pluto["builtfar"]),
            ("residfar", pluto["residfar"]),
            ("commfar", pluto["commfar"]),
            ("facilfar", pluto["facilfar"]),
            ("borocode", nyc["borocode"]),
            ("bbl", nyc["bbl"]),
            ("condono", pluto["condono"]),
            ("tract2010", nyc["tract2010"]),
            ("xcoord", pluto["xcoord"]),
            ("ycoord", pluto["ycoord"]),
            ("zonemap", pluto["zonemap"]),
            ("zmcode", pluto["zmcode"]),
            ("sanborn", pluto["sanborn"]),
            ("taxmap", pluto["taxmap"]),
            ("edesignum", pluto["edesignum"]),
            ("appbbl", pluto["appbbl"]),
            ("appdate", pluto["appdate"]),
            ("plutomapid", pluto["plutomapid"]),
            ("version", pluto["version"]),
            ("mappluto_f", pluto["mappluto_f"]),
            #("shape_leng", pluto["shape_leng"]),
            #("shape_area", pluto["shape_area"]),
            ("wkb_geometry", nyc["parcel"]),
        ])

    def timespan(self):
        return '20' + self.release

    def populate(self):
        input_ = self.input()
        session = current_session()
        incols = ['"{}"::{}'.format(colname, col.get(session).type)
                  for colname, col in self.columns().iteritems()]
        for borough, data in self.input()['data'].iteritems():
            session.execute('''
                INSERT INTO {output}
                SELECT {incols} FROM {intable}
            '''.format(
                output=self.output().table,
                intable=data.table,
                incols=', '.join(incols)
            ))
        session.execute('''create unique index on {output} (bbl)'''.format(
            output=self.output().table
        ))

# class MapPLUTOMetaWrapper(MetaWrapper):
#     release = Parameter()
#
#     params = {
#         'release':['17','16','15']
#     }
#
#     def tables(self):
#         yield MapPLUTO(release=self.release)
