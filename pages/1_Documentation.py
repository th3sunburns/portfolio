import streamlit as st

st.set_page_config(
    page_title='Documentation',
    page_icon='',
)

st.write('# Documentation')

st.container

st.markdown(
    '''

    '''
) 

def page2():
    st.title("Second page")

pg = st.navigation([
    st.Page("page1.py", title="First page", icon="🔥"),
    st.Page(page2, title="Second page", icon=":material/favorite:"),
])
pg.run()