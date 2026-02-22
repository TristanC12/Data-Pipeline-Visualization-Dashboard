import streamlit as st
import altair as alt
import pandas as pl
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

  
st.set_page_config( 
    page_title='COMP 3601 Assignment 1', 
    page_icon='taxi', 
    layout='wide' 
) 

st.title('NYC Yellow Taxi Data Pipeline & Visualization Dashboard.') 

# Big dataframe queries into functions with st.cache_data
@st.cache_data 
def load_data(): 
    df = pl.read_parquet('.\\data\\raw\\yellow_tripdata_2024-01.parquet') 
    lut = pl.read_csv('.\\data\\raw\\taxi_zone_lookup.csv')
    return df, lut

@st.cache_data
def load_filtered(df, ptype, value, bound, startdate, enddate, starthour, endhour):
    if(ptype == "All"):

        if(value == bound):
            trimmed_df = df
        else:
            trimmed_df = df.sample(n=value, random_state=42)
         
        filtered_df = trimmed_df[
            (((df['tpep_pickup_datetime'] >= startdate) & (df['tpep_dropoff_datetime'] <= enddate)) &
            ((df['pickup_hour'] >= starthour) & (df['pickup_hour'] <= endhour)) &
            ((df['payment_type']).all()))
        ]
    else:
        if(value == bound):
            trimmed_df = df
        else:
            trimmed_df = df.sample(n=value, random_state=42) 
        filtered_df = trimmed_df[
            (((df['tpep_pickup_datetime'] >= startdate) & (df['tpep_dropoff_datetime'] <= enddate)) &
            ((df['pickup_hour'] >= starthour) & (df['pickup_hour'] <= endhour)) &
            ((df['payment_type']) == ptype))
        ]
    
    return filtered_df

@st.cache_data
def load_g1_data(filtered_df):
        result = con.execute('''
        SELECT PULocationID, l.Zone,
        COUNT(*) AS total_trips 
        FROM filtered_df
        JOIN lut l
        ON PULocationID = l.LocationID
        GROUP BY PULocationID, l.Zone
        ORDER BY total_trips DESC
        LIMIT 10;
                            
        ''').fetchdf()

        return result

@st.cache_data
def load_g2_data(filtered_df):
        result = con.execute('''
        SELECT pickup_hour,
        AVG(fare_amount) AS average_fare
        FROM filtered_df
        GROUP BY pickup_hour
        ORDER BY pickup_hour;
        ''').fetchdf()

        return result

@st.cache_data
def load_g3_data(filtered_df):
        result = con.execute('''
        SELECT 
        trip_distance,
        trip_duration_minutes
        FROM filtered_df
        WHERE trip_distance < 500;
        ''').fetchdf()

        return result

@st.cache_data
def load_g4_data(filtered_df):
        result = con.execute('''
        SELECT 
        payment_type,
        fare_amount
        FROM filtered_df;
        ''').fetchdf()

        return result

@st.cache_data
def load_g5_data(filtered_df):
        result = con.execute('''
        SELECT 
        pickup_hour,
        DATE_PART('dow', tpep_pickup_datetime) AS day_of_week,
        COUNT(*) as trip_count,     
        FROM filtered_df
        GROUP BY pickup_hour, day_of_week
        ORDER BY pickup_Hour, day_of_week;
        ''').fetchdf()

        return result
  
df, lut = load_data() 

t1, t2, t3, t4 = st.tabs(['Introduction', 'Key Metrics', 'Graphs', 'Filters'])      # Create tabs

t1.header("Introduction")
t2.header("Key Metrics")
t3.header("Graphs")
t4.header("Filters")

with t1:

    st.markdown(
        "## Data Pipeline & Visualization Dashboard\nThis dashboard uses the NYC Yellow Taxi dataset of January 2024. A series of validation, cleaning, transformation and visualization was done to develop insight on various aspects of the service.\n\n"
    )

    km_col, g_col, f_col = st.columns(3)

    with km_col:
        st.markdown(
            "### Key Metrics Tab\n\nThe Key Metrics Tab shows 5 key metrics of the dataset. These metrics include:"
        )
        st.markdown(
            "i) Total Trips"
            )
        st.markdown(
            "ii) Average Fare"
            )
        st.markdown(
            "iii) Total Revenue"
        )
        st.markdown(
            "iv) Average Trip Distance"
        )
        st.markdown(
            "v) Average Trip Duration"
        )
    
    with g_col:
        st.markdown(
            "### The Graphs Tab\n\nThe Graphs Tab shows 5 graphs which use the dataset to visualize some aspects of the dataset. These aspects include:" 
        )
        st.markdown(
            "i) Top 10 Pickup Zones by Trip Count."
            )
        st.markdown(
            "ii) Average Fare by Hour of Day"
            )
        st.markdown(
            "iii) Distribution of Trip Distances"
        )
        st.markdown(
            "iv) Breakdown of Payment Types"
        )
        st.markdown(
            "v) Trips by Day of the Week and Hour"
        )

    with f_col:
        st.markdown(
            f'### The Filters Tab\n\nThe Filters Tab contains 4 simple filters that can be used to capture specific portions of the dataset. These filters are:'
        )
        st.markdown(
            "i) Date Range - Choose a range between the 1st and the 31st of January 2024."
        )
        st.markdown(
            "ii) Hour Range - Choose a range of hours between 0 and 23."
        )
        st.markdown(
            "iii) Payment Type - Only include the specified payment type."
        )
        st.markdown(
            f'iv) Sample Size - Choose the sample size from 0 to {df.shape[0]}.'
        )


with t2:# ======================== Key Metrics Tab ========================

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric('Total Trips', f'{len(df):,}') 
    col2.metric('Average Fare', f'${df["fare_amount"].mean():.2f}') 
    col3.metric('Total Revenue', f'${df["total_amount"].sum():.2f} ') 
    col4.metric('Average Trip Distance', f'{df["trip_distance"].mean():.2f}')
    col5.metric('Average Trip Duration', f'{df['trip_duration_minutes'].mean():.2f} minutes')

with t4:# ======================== Filters Tab ========================

    col_date, col_hour, col_ptype = st.columns(3)

    first_day = datetime(2024, 1, 1)

    last_day = datetime(2024, 1, 31)

    with col_date:
        dates = st.date_input( 
        'Date Range - Choose between the 1st and 31st of January 2024', 
        min_value=first_day, 
        max_value=last_day, 
        value=(first_day, last_day) 
        )

        try:
            startdate, enddate = dates
        except ValueError:
            st.error("You must pick a start and end date")
            with t2:
                st.error("You must pick a start and end date")
                st.stop()

    with col_hour:
        starthour, endhour = st.slider(
        'Hour Range - Choose between hour 0 and 23', 
        min_value=0, 
        max_value=23, 
        value=(0, 23)
    )
        
    with col_ptype:
        payment_options = ['All'] + df['payment_type'].dropna().unique().tolist()
        ptype = st.selectbox(
            'Payment Type - Choose a payment type',payment_options
            )
    
    bound = df.shape[0]

    value = st.number_input(
        f'Sample Size - Choose a number between 0 and {bound}',
        min_value = 0,
        max_value = bound,
        value = (100000),
        placeholder = f'{bound} total rows'
    )

    startdate = pl.to_datetime(startdate)
    enddate = pl.to_datetime(enddate)

    filtered_df = load_filtered(df, ptype, value, bound, startdate, enddate, starthour, endhour)

with t3:# ======================= Graphs Tab =======================

        
        con = duckdb.connect()  # Used DuckDB for speed

        # ======================== Bar chart ========================
        result = load_g1_data(filtered_df)

        # Query reused from the notebook

        print(result)


        bar_chart = alt.Chart(result).mark_bar().encode(
            x = alt.X("Zone", sort = None),
            y = alt.X("total_trips", title = "Total Trips"),
        ).properties(
            height = 600,
        )

        text = bar_chart.mark_text(             # Added number above bars for clarity
            align='center',
            baseline='middle',
            dy=-10,                             # Raise the number
            color = 'grey'
        ).encode(
            text='total_trips:Q'
        )

        # ======================== Line Chart ==============================

        result = load_g2_data(filtered_df)
        # Query reused from the notebook

        line_chart = alt.Chart(result).mark_line().encode(
            x = alt.X("pickup_hour", title="Pickup Hour", sort=None),
            y = alt.X("average_fare", title = "Average Fare")
        ).properties(
            height = 600
        )

        # ======================== Histogram ========================

        result = load_g3_data(filtered_df)

        # Used a second attribute to avoid some issues with the Series type. Just need trip_distance

        print(f'\n{result} ')

        hist_chart = alt.Chart(result).mark_bar().encode(
            x = alt.X("trip_distance:Q", title="Trip Distance", sort = None, bin=alt.Bin(extent = [0, 100],step = 2), axis=alt.Axis(values=list(range(0, 102, 2)))), 
            y = alt.Y("count()", title = "Total Trips"),
            tooltip = ["trip_distance","count()"]
            
        ).properties(
            height = 600
        )

        # ========================  Second Bar Chart ========================

        result = load_g4_data(filtered_df)

        bar_chart2 = alt.Chart(result).mark_bar(size = 40).encode(
            x = alt.X("payment_type:Q", title="Payment Types" ,sort = None, axis = alt.Axis(values=[1,2,3,4])),
            y = alt.X("count()", title = "Total Payments")
        ).properties(
            height = 600
        )
        
        text2 = bar_chart2.mark_text(       # Added number above bars for clarity here as well
            align='center',
            baseline='middle',
            dy=-10,                         # Raise the number
            color = 'grey'
        ).encode(
            text='count():Q'
        )

        # ========================  Heatmap ========================

        result = load_g5_data(filtered_df)

        print(result)

        heat_map = alt.Chart(result).mark_rect().encode(
            x=alt.X('pickup_hour:O', title='Pickup Hour'),
            y=alt.Y('day_of_week:O', title='Day of Week (Sunday is 0)'),
            color=alt.Color('trip_count:Q', title = 'Trip Count' ,scale=alt.Scale(scheme='redyellowgreen'))
        ).properties(
            height = 600
        )

        # ======================== Graphs separated by borders ==============================

        with st.container(border = True):
            st.markdown(
                "### Top 10 Pickup Zones by Trip Count"
            )
  
            st.altair_chart(bar_chart + text, use_container_width = True)
            st.markdown(
                "#### High concentration around Midtown and the Upper East Side."
                )
            st.markdown(
                "The areas with the most demand are the eastern areas of the city. There is a relatively sharp drop after the Upper East Side South area which is followed by the Midtown East, Penn Station/Madison Square West and Times Square. This portion of the distribution also includes the JFK International Airport which holds the highest frequency of trips. This may indicate that trips between the airport and major tourist attractions such as Central Park, Times Square and other landmarks are major drivers in the revenue generated by taxis."
            )

        with st.container(border = True):
            st.markdown(
                "### Average Fare by Hour of Day"
            )
            st.altair_chart(line_chart, use_container_width = True)
            st.markdown(
                "#### Longer Trips on Mornings."
                )
            st.markdown(
                "This could be due to work as people travel earlier to make up for distance. There may not be a matching spike on evenings as people may leave work at random points in the day and the averages may be lowered from others taking shorter trips after the interval."
            )

        with st.container(border = True):
            st.markdown(
                "### Distribution of Trip Distances"
            )
            st.altair_chart(hist_chart, use_container_width = True)
            st.markdown(
                "#### Mostly Trips Within the City."
                )
            st.markdown(
                "As the majority of trips are concentrated around the 0 to 5 mile range, it appears that the service is very rarely used for long trips. Considering the first graph that shows the areas with the highest trips, it would seem that most the revenue is generated from trips in and around the Midtown Center."
            )

        with st.container(border = True):
            st.markdown(
                "### Breakdown of Payment Types"
            )
            st.altair_chart((bar_chart2 + text2), use_container_width = True)
            st.markdown(
                "#### Aspects of the Service are Dependent on Payment Type 1"
                )
            st.markdown(
                "As the vast majority of payments are done with method 1, it's availability is vital. Fare payments rely so much on this type of payment that one may conclude that revenue is dependent on the availability of this type."
            )

        with st.container(border = True):
            st.markdown(
                "### Trips by Day of the Week and Hour"
            )
            st.altair_chart(heat_map, use_container_width = True)
            st.markdown(
                "#### More Trips with Shorter Distances on evenings."
                )
            st.markdown(
                "Considering the previous graph that shows the higher average fares on mornings, this graph indicates that taxi generate vastly more money around the evening times. The fare averages are lower but the amount of trips are much higher. This graph may also indicate that people tend to use a different mode of transportation on mornings."
            )
