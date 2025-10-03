# import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import date_trunc, col, concat, lit, date_part
import pandas as pd
import numpy as np
import altair as alt
from decimal import *

# set page to wide mode
st.set_page_config(layout="wide")

# get list of banks for dropdown
def get_banks(_session):
    query = """
    SELECT sc.INSTITUTIONNAME,sc.INSTITUTIONID
    FROM MI_XPRESSCLOUD.XPRESSFEED.snlCorp sc
    JOIN MI_XPRESSCLOUD.XPRESSFEED.snlInstnReference ir on ir.INSTITUTIONID = sc.INSTITUTIONID
    WHERE ir.SNLREGDOMAINID IN (1,2,3,4,6,7,9,10)
    AND sc.INSTITUTIONSTATUS IN ('Operating','Operating Subsidiary')
    """
    data = _session.sql(query)
    
    return data

def calc_lat_lon_range(center_lat,center_long,radius_miles):

    '''Function to calculate the maximum and minimum latitudes and longitudes \
        for a given radius and a given latitude and longitude pair.'''

    # radius of the Earth in miles
    earth_radius = 3956

    # convert radius from miles to radians
    radius_radians = radius_miles / earth_radius

    # calculate the latitude range
    min_lat = center_lat - np.degrees(radius_radians)
    max_lat = center_lat + np.degrees(radius_radians)

    # calculate the longitude range
    min_long = center_long - np.degrees(radius_radians / np.cos(np.radians(center_lat)))
    max_long = center_long + np.degrees(radius_radians / np.cos(np.radians(center_lat)))

    # convert values
    min_lat = Decimal(str(min_lat)).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
    max_lat = Decimal(str(max_lat)).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
    min_long = Decimal(str(min_long)).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
    max_long = Decimal(str(max_long)).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
    
    return (min_lat,min_long,max_lat,max_long)

# write directly to the app
st.title(f"Find Businesses Near A Bank's Branches")

# get current credentials
session = get_active_session()

bank_list = get_banks(session).to_pandas()['INSTITUTIONNAME'].to_list()

# create selectbox
selected_bank = st.selectbox("Bank:",options=bank_list,index=None)

if st.button("Run query"):
    with st.spinner("Processing..."):

        # latitude dataitemid = 16, longitude = 17

        # get bank branch data, limit to 5 branches
        #https://www.marketplace.spglobal.com/en/datasets/snl-us-bank-branch-data-(38)

        branch_query =  """
        SELECT bo.INSTITUTIONID,b.ZIP,bd1.DATAITEMVALUE as LATITUDE,bd2.DATAITEMVALUE as LONGITUDE,td.DATAITEMVALUE as STREET,c.CITY,s.STATENAME
        FROM MI_XPRESSCLOUD.XPRESSFEED.snlBranch b
        JOIN MI_XPRESSCLOUD.XPRESSFEED.snlBranchData bd1 on bd1.BRANCHID = b.BRANCHID
        JOIN MI_XPRESSCLOUD.XPRESSFEED.snlBranchData bd2 on bd2.BRANCHID = b.BRANCHID
        JOIN MI_XPRESSCLOUD.XPRESSFEED.snlBranchOwnership bo on bo.BRANCHID = b.BRANCHID
        JOIN MI_XPRESSCLOUD.XPRESSFEED.snlCorp sc on sc.INSTITUTIONID = bo.INSTITUTIONID
        JOIN MI_XPRESSCLOUD.XPRESSFEED.snlbranchtextdata td on td.BRANCHID = b.BRANCHID
        JOIN MI_XPRESSCLOUD.XPRESSFEED.snlCityAsReported c on c.CITYASREPORTEDID = b.CITYASREPORTEDID
        JOIN MI_XPRESSCLOUD.XPRESSFEED.snlState s on s.STATEID = c.STATEID
        WHERE sc.INSTITUTIONNAME = ?
        AND bo.MOSTRECENTCURRENTOWNER = 1
        AND bd1.DATAITEMID = 16
        AND bd2.DATAITEMID = 17
        AND td.DATAITEMID = 120
        LIMIT 5;
        """

        branch_df = session.sql(branch_query,params=[selected_bank]).to_pandas()

        for key,val in branch_df.iterrows():
            branch_street = val['STREET']
            branch_city = val['CITY']
            branch_state = val['STATENAME']
            branch_zip = val['ZIP']
            center_lat = val['LATITUDE']
            center_long = val['LONGITUDE']
            radius_miles = 10
            min_lat,min_long,max_lat,max_long = calc_lat_lon_range(center_lat,center_long,radius_miles=radius_miles)

            # get business listings data, limit to 10 contacts
            #https://www.marketplace.spglobal.com/en/datasets/business-listings-(1725359234)

            bl_query = """
            SELECT bc.BLFULLNAME as FULLNAME,bc.BLEMAIL as EMAIL,bc.BLTITLE as TITLE,bd.BLCOMPANYNAME as COMPANY
            FROM MI_XPRESSCLOUD.XPRESSFEED.BLBusinessDetail bd
            JOIN MI_XPRESSCLOUD.XPRESSFEED.BLBusinessContact bc on bc.BUSINESSLISTINGID = bd.BUSINESSLISTINGID
            WHERE (bd.LATITUDE BETWEEN ? AND ?)
            AND (bd.LONGITUDE BETWEEN ? AND ?)
            AND bd.BLCOMPANYNAME IS NOT NULL 
            AND bc.BLEMAIL IS NOT NULL
            limit 10;
            """
            bl_df = session.sql(bl_query,params=(min_lat,max_lat,min_long,max_long)).to_pandas()
            st.subheader(f"Branch info: {branch_street}, {branch_city}, {branch_state}, {branch_zip}")

            st.dataframe(bl_df)