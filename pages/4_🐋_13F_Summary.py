import streamlit as st
import pandas as pd
import io
import openpyxl
from streamlit_google_auth import Authenticate

@st.cache_data
def load_ticker_data(ticker_file):
    return pd.read_excel(ticker_file)

def generate_holdings_summary(df_tickers, ww_file):
    # Read the Whalewisdom export excel file
    df_ww_export = pd.read_excel(ww_file, skiprows=3)

    # Perform left join on df_tickers and df_ww_export
    df_merged = pd.merge(df_tickers, df_ww_export, left_on='Ticker', right_on='Symbol', how='left')

    # Convert 'Market Value' to integer
    df_merged['Market Value'] = df_merged['Market Value'].fillna(0).astype(int)
    
    # Slice df_merged to show Ticker, Market Value, and Type
    df_sliced = df_merged[['Ticker', 'Market Value', 'Type']]
    # Only drop rows where Ticker or Market Value is NA, allow Type to be NA
    df_sliced = df_sliced.dropna(subset=['Ticker', 'Market Value'])

    # Drop rows where Market Value is zero
    df_sliced = df_sliced[df_sliced['Market Value'] != 0]

    df_sliced = df_sliced.sort_values(by='Market Value', ascending=False)
    df_sliced['Market Value'] = df_sliced['Market Value'].apply(lambda x: "{:,}".format(x))
    
    # Create a new column 'Text' that combines 'Ticker' and 'Market Value'
    df_sliced['Text'] = df_sliced['Ticker'] +' '+ df_sliced['Type'].fillna('') + ' $' + df_sliced['Market Value']

    # Join all elements in the 'Text' column into a single string, separated by newlines
    text_chunk = '\n'.join(df_sliced['Text'])

    return text_chunk



def main():
    st.title("Holdings Summary Generator")
    st.write("""
    Purpose: This page extracts the relevant holdings information from 13F (via Whale Wisdom exports) and outputs a text summary.

    Steps:
    1. Upload the tickers that you are interested in. The Excel file needs to include a column named 'Ticker'.
    2. Upload the Excel exports from Whale Wisdom.
    3. Click 'Generate Summary' to process the data and view the results.
    """)

    # Master ticker file upload
    if 'ticker_data' not in st.session_state:
        ticker_file = st.file_uploader("Upload Master ETF Data Pull Excel file", type=["xlsx"])
        if ticker_file:
            st.session_state.ticker_data = load_ticker_data(ticker_file)
            st.success("Master ETF Data Pull file loaded successfully!")

    # Whalewisdom file upload
    ww_file = st.file_uploader("Upload Whalewisdom Export Excel file", type=["xlsx"])

    if 'ticker_data' in st.session_state and ww_file:
        if st.button("Generate Summary"):
            try:
                result = generate_holdings_summary(st.session_state.ticker_data, ww_file)
                st.text_area("Holdings Summary", result, height=400)
                st.download_button("Download Summary", result, "holdings_summary.txt")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
    elif 'ticker_data' not in st.session_state:
        st.info("Please upload the Master ETF Data Pull file first.")
    else:
        st.info("Please upload a Whalewisdom Export file to generate the summary.")

def check_user_domain(user_email):
    allowed_domain = 'vestfin.com'
    return user_email.split('@')[1] == allowed_domain


authenticator = Authenticate(
        secret_credentials_path='google_credentials.json',
        cookie_name='my_cookie_name',
        cookie_key='this_is_secret',
        redirect_uri='https://vest-sales-tools.streamlit.app/',
     )

if __name__ == '__main__':
    # Check if the user is already authenticated
    authenticator.check_authentification()

    # Display the main content only if the user is authenticated and has the correct domain
    if st.session_state['connected']:
        user_email = st.session_state['user_info'].get('email')
        if check_user_domain(user_email):
            if st.button("Logout", key="logout_button"):
                authenticator.logout()
                st.rerun()
            main()
        else:
            st.error("Access denied. Only users with @vestfin.com email addresses are allowed.")
    else:
        st.markdown("<h1 style='text-align: center; font-size: 2.5em;'>Welcome to Vest Sales Tools.<br>Please log in with your Vest Google Account.</h1>", unsafe_allow_html=True)
        authenticator.login()

    # After login, check the user's email domain
    if st.session_state['connected'] and not check_user_domain(st.session_state.get('user_email', '')):
        st.error("Access denied. Only users with @vestfin.com email addresses are allowed.")
        authenticator.logout()
        st.rerun()