import pymssql
import streamlit as st
import datetime as dt 
import pandas as pd 
import numpy as np
from PIL import Image
from styles import style
import os
import json
import mistletoe
import markdown
import math
import streamlit.components.v1 as components
import shelve

def hive_sql(SQLCommand, limit):
    db = os.environ['HIVESQL'].split()
    conn = pymssql.connect(server=db[0], user=db[1], password=db[2], database=db[3])
    cursor = conn.cursor()
    cursor.execute(SQLCommand)
    result = cursor.fetchmany(limit)
    conn.close()
    return result

def hive_per_vest():
    SQLCommand = '''
    SELECT hive_per_vest
    FROM DynamicGlobalProperties
    '''
    hpv = hive_sql(SQLCommand,10)[0][0]
    return hpv

def likeCommand(voters, column, combiner, included):
    if len(voters) == 0:
        return ''
    voters_string = ''
    combiner = combiner+' '
    for i in range(len(voters)):
        if i == len(voters)-1:
            combiner = ''
        voters_string += included + column+' LIKE(\'%\"'+voters[i]+'\"%\') '+combiner
    voters_string = 'AND ('+voters_string+')'
    return voters_string

def likeCommandTwo(voters, column, combiner, included):
    if len(voters) == 0:
        return ''
    voters_string = ''
    combiner = combiner+' '
    for i in range(len(voters)):
        if i == len(voters)-1:
            combiner = ''
        voters_string += included + column+' LIKE(\'% '+voters[i]+' %\') '+combiner
    voters_string = 'AND ('+voters_string+')'
    return voters_string

def inCommand(listed, logic):
    if len(listed) == 0:
        return ''
    my_string = 'AND {} c.author IN ('.format(logic)
    combiner = ","
    for i in range(len(listed)):
        if i == len(listed)-1:
            combiner = ')\n'
        my_string += "'"+listed[i]+"'"+combiner
    return my_string

def include_tags_cond(include_tags, any_all_in):
    include_tags = include_tags.split()
    if any_all_in == 'Any':
        includedTagsCond = likeCommand(include_tags, "c.json_metadata", "or", "")
    if any_all_in == 'All':
        includedTagsCond = likeCommand(include_tags, "c.json_metadata", "and", "")
    return includedTagsCond

def exclude_tags_cond(exclude_tags):
    exclude_tags = exclude_tags.split()
    excludedTagsCond = likeCommand(exclude_tags, "c.json_metadata", "and", "NOT ")
    return excludedTagsCond

def include_authors_cond(include_authors):
    include_authors = include_authors.split()
    includedAuthors = inCommand(include_authors, '')
    return includedAuthors

def exclude_authors_cond(exclude_authors):
    exclude_authors = exclude_authors.split()
    excludedAuthors = inCommand(exclude_authors, 'NOT')
    return excludedAuthors

def include_voters_cond(include_voters):
    include_voters = include_voters.split()
    includedVoters = likeCommand(include_voters, "c.active_votes", "or", "")
    return includedVoters

def exclude_voters_cond(exclude_voters):
    exclude_voters = exclude_voters.split()
    excludedVoters = likeCommand(exclude_voters, "c.active_votes", "and", "NOT ")
    return excludedVoters

def title_contains_cond(title_contains):
    title_contains = title_contains.split()
    titleContains = likeCommandTwo(title_contains, "c.title", "and", "")
    return titleContains

def title_contains_cond_not(title_contains_not):
    title_contains_not = title_contains_not.split()
    titleContainsNot = likeCommandTwo(title_contains_not, "c.title", "and", "NOT ")
    return titleContainsNot

def body_contains_cond(body_contains):
    body_contains = body_contains.split()
    bodyContains = likeCommandTwo(body_contains, "c.body", "and", "")
    return bodyContains

def body_contains_cond_not(body_contains_not):
    body_contains_not = body_contains_not.split()
    bodyContainsNot = likeCommandTwo(body_contains_not, "c.body", "and", "NOT ")
    return bodyContainsNot

def get_posts(limit, start_dt, end_dt, chosen_app, external_link, 
            min_rep, max_rep, min_len, max_len, min_pay, max_pay, 
            min_votes, max_votes, min_comments, max_comments, 
            min_hp, max_hp, include_tags, any_all_in, exclude_tags,
            include_authors, exclude_authors, include_voters, exclude_voters,
            title_contains, title_contains_not, body_contains, body_contains_not,
            show_hide_index):

    if app == 'ALL':
        app_condition = ''
    else:
        app_condition = ''' AND c.json_metadata LIKE('%"app":"{}%') '''.format(app)

    hpv = hive_per_vest()
    incl_tags_cond = include_tags_cond(include_tags, any_all_in)
    excl_tags_cond = exclude_tags_cond(exclude_tags)
    incl_au_cond = include_authors_cond(include_authors)
    excl_au_cond = exclude_authors_cond(exclude_authors)
    incl_vt_cond = include_voters_cond(include_voters)
    excl_vt_cond = exclude_voters_cond(exclude_voters)
    title_cont_cond = title_contains_cond(title_contains)
    title_cont_cond_not = title_contains_cond_not(title_contains_not)
    body_cont_cond = body_contains_cond(body_contains)
    body_cont_cond_not = body_contains_cond_not(body_contains_not)

    SQLCommand = f'''
    SELECT c.title, c.author, a.reputation_ui, c.created, 'https://' + '{external_link}' + c.url, c.body,
            c.pending_payout_value, c.children, c.net_votes 
    FROM Comments c
    JOIN Accounts a
    ON c.author = a.name
    WHERE c.created BETWEEN CAST('{start_dt}' as datetime) AND CAST('{end_dt}' as datetime)
    AND c.parent_author = ''
    {app_condition}
    AND a.reputation_ui BETWEEN {min_rep} AND {max_rep}
    AND c.body_length BETWEEN {min_len} AND {max_len}
    AND c.pending_payout_value BETWEEN {min_pay} AND {max_pay}
    AND c.net_votes BETWEEN {min_votes} AND {max_votes}
    AND c.children BETWEEN {min_comments} AND {max_comments}
    AND (a.vesting_shares + a.received_vesting_shares) * {hpv} BETWEEN {min_hp} AND {max_hp}
    {incl_tags_cond}
    {excl_tags_cond}
    {incl_au_cond}
    {excl_au_cond}
    {incl_vt_cond}
    {excl_vt_cond}
    {title_cont_cond}
    {title_cont_cond_not}
    {body_cont_cond}
    {body_cont_cond_not}
    ORDER BY c.created DESC
    '''

    result = hive_sql(SQLCommand, limit)
    main_content = st.empty()
    result_count = 1
    blog_post = []
    exp_parameter = False if show_hide_index == 0 else True
    for p in result:
        title = p[0].replace("\n","")
        body = markdown.markdown(p[5])
        body = mistletoe.markdown(body)
        cl_id = 'close_post'+str(result_count)
        text = f'''
        <div class="post_top">
        <span class="result_count">{result_count}</span>
        <span class="author"> By @{p[1]}</span>
        <span class="rep"> ({math.floor(p[2])})</span>
        <span class="datetime"> {p[3]}</span>
        <span class="pay"> &#128176; - ${round(p[6],2)} </span>
        <span class="comments"> &#128172; - {p[7]} </span>
        <span class="upvotes"> &#128077; - {p[8]} </span>
        <span class="external_link"><a href="{p[4]}" target="_blank"> {external_link} </a></span>
        </div>
        <h1 class="post_title">{title}</h1>

        '''
        #<div class="post_body">{body}</div>
        
        with st.beta_container():
            st.markdown(text, unsafe_allow_html=True)
            with st.beta_expander(label='Show/Hide Content', expanded=exp_parameter):
                st.markdown(body, unsafe_allow_html=True)
        result_count += 1

def get_default_parameters():
    end_dt = dt.datetime.now(dt.timezone.utc)
    start_dt = end_dt - dt.timedelta(days=1)
    end_date = end_dt.date()
    end_time = end_dt.time()
    start_date = start_dt.date()
    start_time = start_dt.time()

    default_para = {"posts_limit":300,
                    "start_date":start_date,
                    "start_time":start_time,
                    "end_date":end_date,
                    "end_time":end_time,
                    "app_index":0,
                    "external_link":'hive.blog',
                    "min_rep":30,
                    "max_rep":75,
                    "min_len":5000,
                    "max_len":15000,
                    "min_pay":0,
                    "max_pay":3,
                    "min_votes":0,
                    "max_votes":2000,
                    "min_comments":0,
                    "max_comments":2000,
                    "min_hp":0,
                    "max_hp":20_000_000,
                    "include_tags":"",
                    "any_all_index":0,
                    "exclude_tags":"nsfw",
                    "include_authors":"",
                    "exclude_authors":"",
                    "include_voters":"",
                    "exclude_voters":"",
                    "title_contains":"",
                    "title_contains_not":"",
                    "body_contains":"",
                    "body_contains_not":"",
                    "show_hide_index":0}
    return default_para

def get_current_parameters(p):
    para = {"posts_limit":p[0],
            "start_date":p[1],
            "start_time":p[2],
            "end_date":p[3],
            "end_time":p[4],
            "app_index":p[5],
            "external_link":p[6],
            "min_rep":p[7],
            "max_rep":p[8],
            "min_len":p[9],
            "max_len":p[10],
            "min_pay":p[11],
            "max_pay":p[12],
            "min_votes":p[13],
            "max_votes":p[14],
            "min_comments":p[15],
            "max_comments":p[16],
            "min_hp":p[17],
            "max_hp":p[18],
            "include_tags":p[19],
            "any_all_index":p[20],
            "exclude_tags":p[21],
            "include_authors":p[22],
            "exclude_authors":p[23],
            "include_voters":p[24],
            "exclude_voters":p[25],
            "title_contains":p[26],
            "title_contains_not":p[27],
            "body_contains":p[28],
            "body_contains_not":p[29],
            "show_hide_index":p[30]}
    return para

if __name__ == '__main__':
    #Initial page configuration
    st.set_page_config(
        page_title="Librarian Search",
        page_icon="hive_logo.png",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    #Styling the page. Style variable is imported from a separate styles.py file
    st.sidebar.markdown(style, unsafe_allow_html=True)

    # hive_searcher = st.empty()
    # hive_searcher = components.iframe(src="https://hivesearcher.com/", height=600)

    

    #Display Sidebar Logo
    logo = Image.open('librarian.png')
    st.sidebar.image(logo, width=300)

    parameters_input = st.sidebar.text_input(label='Previously saved search parameters name:', value='default')

    if parameters_input != 'default':
        with shelve.open('sorting_parameters') as db:
            if parameters_input in db:
                parameters = db[parameters_input]
            else:
                st.sidebar.warning('Sorting name doesn\'t exist. Please enter correct name or type "default".')
                st.stop()
    else:
        parameters = get_default_parameters()

    #Posts Limit
    posts_limit = st.sidebar.number_input(label='Number of Posts: (min:10, max:500)', 
                                            min_value=10, max_value=500, value=parameters["posts_limit"])
    
    #Dates and Times
    # end_dt = dt.datetime.now(dt.timezone.utc)
    # start_dt = end_dt - dt.timedelta(days=7)
    # end_date = end_dt.date()
    # end_time = end_dt.time()
    # start_date = start_dt.date()
    # start_time = start_dt.time()
    chosen_start_date = st.sidebar.date_input(label='Start Date (UTC):', value=parameters["start_date"])
    chosen_start_time = st.sidebar.time_input(label='Start Time (UTC):', value=parameters["start_time"])
    chosen_end_date = st.sidebar.date_input(label='End Date (UTC):', value=parameters["end_date"])
    chosen_end_time = st.sidebar.time_input(label='End Time (UTC):', value=parameters["end_time"])
    start_dt = dt.datetime.combine(chosen_start_date, chosen_start_time)
    end_dt = dt.datetime.combine(chosen_end_date, chosen_end_time)
    start_dt = str(start_dt).split('.')[0]
    end_dt = str(end_dt).split('.')[0]

    #Apps used to post & external link
    apps = ['ALL', 'HIVEBLOG', 'PEAKD', 'ECENCY', 'LEOFINANCE', 'STEMGEEKS', 'NEOXIANCITY', 'PALNET', 'CLICKTRACKPROFIT', 
            'CREATIVECOIN', 'TRAVELFEED', 'ACTIFIT',  'SPORTSTALKSOCIAL', '3SPEAK','DBUZZ', 'VIMM', 'DPOLL', 'OTHER']
    ext_links = ['hive.blog', 'peakd.com', 'ecency.com', 'leofinance.io', 'stemgeeks.net', 'neoxian.city', 'palnet.io',
                'ctptalk.com', 'creativecoin.xyz', 'travelfeed.io', 'actifit.io', 'sportstalksocial.com', '3speak.co', 'd.buzz', 'vimm.tv','dpoll.io']
    apps_no_all = apps[1:]
    app = st.sidebar.selectbox(label='App used to post:', 
                                options= apps, 
                                index=parameters["app_index"])
    app_index = 0 if app == 'ALL' else apps_no_all.index(app)
    if app == 'OTHER':
        chosen_app = st.sidebar.text_input('Please enter the App name:', value='')
        external_link = 'hive.blog'
        if not chosen_app:
            st.sidebar.warning('Please input an App name or choose from the list.')
            st.stop()
    else:
        chosen_app = app
        external_link = st.sidebar.selectbox(label='External link', 
                                        options=ext_links,
                                        index=app_index)
    if external_link in ['3speak.co', 'd.buzz', 'vimm.tv','dpoll.io']:
        external_link = 'hive.blog'

    #Min & Max
    min_rep = st.sidebar.number_input(label='Min Author Rep:', value=parameters["min_rep"])
    max_rep = st.sidebar.number_input(label='Max Author Rep:', value=parameters["max_rep"])

    min_len = st.sidebar.number_input(label='Min Post Characters/Length:', value=parameters["min_len"])
    max_len = st.sidebar.number_input(label='Max Post Characters/Length:', value=parameters["max_len"])

    min_pay = st.sidebar.number_input(label='Min Pending Payout:', value=parameters["min_pay"])
    max_pay = st.sidebar.number_input(label='Max Pending Payout:', value=parameters["max_pay"])

    min_votes = st.sidebar.number_input(label='Min Number of Votes:', value=parameters["min_votes"])
    max_votes = st.sidebar.number_input(label='Max Number of Votes:', value=parameters["max_votes"])

    min_comments = st.sidebar.number_input(label='Min Number of Comments:', value=parameters["min_comments"])
    max_comments = st.sidebar.number_input(label='Max Number of Comments:', value=parameters["max_comments"])

    min_hp = st.sidebar.number_input(label='Min Author Hive Power:', value=parameters["min_hp"])
    max_hp = st.sidebar.number_input(label='Max Author Hive Power:', value=parameters["max_hp"])

    # min_images = st.sidebar.number_input(label='Min Number of Images:', value=0)
    # max_images = st.sidebar.number_input(label='Max Number of Images:', value=100)

    #Include & Exclude
    include_tags = st.sidebar.text_area(label='Include Tags (separated with space)', value=parameters["include_tags"], height=2)
    any_all_in = st.sidebar.selectbox(label='Any or All tags should be included?', options=["Any", "All"], index=parameters["any_all_index"])
    exclude_tags = st.sidebar.text_area(label='Exclude Tags (separated with space)', value=parameters["exclude_tags"], height=2)

    include_authors = st.sidebar.text_area(label='Include Authors (separated with space)', value=parameters["include_authors"], height=2)
    exclude_authors = st.sidebar.text_area(label='Exclude Authors (separated with space)', value=parameters["exclude_authors"], height=2)

    include_voters = st.sidebar.text_area(label='Include Voters (separated with space)', value=parameters["include_voters"], height=2)
    exclude_voters = st.sidebar.text_area(label='Exclude Voters (separated with space)', value=parameters["exclude_voters"], height=2)

    title_contains = st.sidebar.text_input(label='Title Contains: (separated with space)', value=parameters["title_contains"])
    title_contains_not = st.sidebar.text_input(label='Title Does Not Contain: (separated with space)', value=parameters["title_contains_not"])

    body_contains = st.sidebar.text_input(label='Body Contains: (separated with space)', value=parameters["body_contains"])
    body_contains_not = st.sidebar.text_input(label='Body Does Not Contain: (separated with space)', value=parameters["body_contains_not"])

    show_hide_content = st.sidebar.radio(label='How to display search results?', options=["Title only", "Full Content"], index=parameters["show_hide_index"])
    show_hide_index = 0 if show_hide_content == 'Title only' else 1
    get_posts_button = st.sidebar.button('Get Posts')

    if get_posts_button:
        get_posts(posts_limit, start_dt, end_dt, chosen_app, external_link, 
            min_rep, max_rep, min_len, max_len, min_pay, max_pay, 
            min_votes, max_votes, min_comments, max_comments, 
            min_hp, max_hp, include_tags, any_all_in, exclude_tags,
            include_authors, exclude_authors, include_voters, exclude_voters,
            title_contains, title_contains_not, body_contains, body_contains_not,
            show_hide_index)

    st.sidebar.markdown('<hr>', unsafe_allow_html=True)

    new_parameters_name = st.sidebar.text_input(label='Save search parameters as: (e.g. geekgirl-crypto)',
                                              value='')
    save_parameters = st.sidebar.button('Save Parameters')
    if save_parameters:
        with shelve.open('sorting_parameters') as db:
            # if new_parameters_name in db:
            #     st.sidebar.warning('The name already exists. Please choose a different name.')
            #     st.stop()
            # else:
            any_all_index = 0 if any_all_in == 'Any' else 1
            current_parameter_variables = [posts_limit,
                chosen_start_date,
                chosen_start_time,
                chosen_end_date,
                chosen_end_time,
                apps.index(chosen_app),
                external_link,
                min_rep,
                max_rep,
                min_len,
                max_len,
                min_pay,
                max_pay,
                min_votes,
                max_votes,
                min_comments,
                max_comments,
                min_hp,
                max_hp,
                include_tags,
                any_all_index,
                exclude_tags,
                include_authors,
                exclude_authors,
                include_voters,
                exclude_voters,
                title_contains,
                title_contains_not,
                body_contains,
                body_contains_not,
                show_hide_index]
            current_parameters = get_current_parameters(current_parameter_variables)
            db[new_parameters_name] = current_parameters
            st.sidebar.success('{} saved! Now you can use this name to autocomplete the sorting parameters.'.format(new_parameters_name))

    # st.sidebar.write('Already created sorting parameters:')
   
    # with shelve.open('sorting_parameters') as db:
    #     x = 0
    #     for name in db:
    #         st.sidebar.write(str(x) + ' - ' + name)
    #         x+=1
    #         if x > 100:
    #             break
