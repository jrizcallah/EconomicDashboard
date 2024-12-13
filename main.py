import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
import os

st.set_page_config(
    page_title='Colorado Economic Dashboard',
    layout='wide',
    initial_sidebar_state='collapsed'
)

main_graph_data = pd.read_csv(st.secrets.blobs.GRAPH_DATA_BLOB_URL)
main_graph_data = main_graph_data.reset_index()
main_graph_data['month'] = pd.to_datetime(main_graph_data['month'])

business_entity_data = pd.read_csv(st.secrets.blobs.BUSINESS_ENTITIES_BLOB_URL)
business_entity_data['entityformdate'] = pd.to_datetime(business_entity_data['entityformdate'])


with st.sidebar:
    st.title('CO Economics Dashboard')
    selection_list = ['Overview',
                      'Business Filings Summary',
                      'Filings Detail',
                      'Economic Indicators']
    selected = st.selectbox('Select a page', selection_list, 
                            index=len(selection_list)-1)


def make_main_graph(input_df, 
                    input_color_theme):
    main_graph = alt.Chart(input_df).mark_line().encode(
        x=alt.X('month:T',
               axis=alt.Axis(title='Month',
                            titleFontSize=18, titlePadding=15, titleFontWeight=900,
                            labelAngle=0)),
        y=alt.Y('value:Q',
                axis=alt.Axis(title='',
                              titleFontSize=18, titlePadding=15, titleFontWeight=900)),
        color=alt.Color('series:N', legend=None, scale=alt.Scale(scheme=input_color_theme)).legend(orient='right'),
        strokeWidth=alt.value(1.5)
    ).properties(width=900).configure_axis(labelFontSize=12,
                                           titleFontSize=12)
    return main_graph

outside_columns = st.columns((0.7, 0.3), gap='small')

with outside_columns[0]:
    with st.container():
        cols1 = st.columns(3, gap='medium')
        with cols1[0]:
            with st.container(border=True, height=250):
                st.write(':orange[Monthly New Entity Filings]')
                monthly_business_entity_data = business_entity_data.copy()
                monthly_business_entity_data['month_year'] = monthly_business_entity_data['entityformdate'].dt.month.astype(str) + '-' + monthly_business_entity_data['entityformdate'].dt.year.astype(str)
                monthly_business_entity_data['month_year'] = pd.to_datetime(monthly_business_entity_data['month_year'],
                                                                            format='%m-%Y')
                monthly_business_entity_data = monthly_business_entity_data[['month_year', 'count_entityid']].groupby('month_year').sum()
                most_recent_date = monthly_business_entity_data.index.max()
                most_recent_full_month = most_recent_date - pd.tseries.offsets.DateOffset(months=1)
                string_date = most_recent_full_month.strftime('%B, %Y')
                st.write(f'Up-to-date as of :gray[{string_date}]')
                whole_value = monthly_business_entity_data.loc[most_recent_full_month].iloc[0]
                percent_change = monthly_business_entity_data.pct_change(12).loc[most_recent_full_month].iloc[0]
                st.metric(label='', value=f"{whole_value:,}", delta=f"{percent_change:.2%}", 
                          help=":gray[Year-over-Year]", label_visibility='collapsed')
        
        with cols1[1]:
            with st.container(border=True, height=250):
                st.write(':orange[Total Entities in Good Standing]')
                good_standing = business_entity_data[business_entity_data['entitystatus'] == 'Good Standing']
                good_standing = good_standing.set_index('entityformdate')['count_entityid']
                most_recent_date = good_standing.index.max()
                most_recent_value = good_standing.sum()
                string_date = most_recent_date.strftime('%B %d, %Y')
                st.write(f'Most recent filing on :gray[{string_date}]')
                st.metric(label='', value=f'{most_recent_value:,}', 
                          help=':gray[Year-over-Year]', label_visibility='collapsed')
        
        with cols1[2]:
            with st.container(border=True, height=250):
                st.write(':orange[Total Entities in Delinquency]')
                delinquent = business_entity_data[business_entity_data['entitystatus'] == 'Delinquent']
                delinquent = delinquent.set_index('entityformdate')['count_entityid']
                most_recent_date = delinquent.index.max()
                most_recent_value = delinquent.sum()
                string_date = most_recent_date.strftime('%B %d, %Y')
                st.write(f'Most recent filing on :gray[{string_date}]')
                st.metric(label='', value=f'{most_recent_value:,}',
                          help=':gray[Year-over-Year]', label_visibility='collapsed')


    with st.container():
        st.write('#### Colorado :blue[**Business Formation Statistics**] and :orange[**New Entity Filings**]')
        main_graph = make_main_graph(main_graph_data, 'viridis')
        st.altair_chart(main_graph)

with outside_columns[1]:
    with st.container():
        status_counts = business_entity_data['entitystatus'].value_counts().reset_index()
        st.dataframe(status_counts,
                     hide_index=True,
                     column_config={
                         'entitystatus': st.column_config.TextColumn('Status'),
                         'count': st.column_config.ProgressColumn(
                             'Count', format='%f', 
                             min_value=0, max_value=max(status_counts['count'])
                         )
                     },
                     use_container_width=True,
                     height=300)
        
        city_counts = business_entity_data.loc[business_entity_data['entitystatus'] == 'Good Standing', 'principalcity'].value_counts()
        city_counts = city_counts[city_counts > 1000].reset_index()
        st.dataframe(city_counts,
                     hide_index=True,
                     column_config={
                         'principalcity': st.column_config.TextColumn('City'),
                         'count': st.column_config.ProgressColumn(
                             'Entities in Good Standing', format='%f',
                             min_value=0, max_value=max(city_counts['count'])
                         )
                     },
                     use_container_width=True,
                     height=300)
