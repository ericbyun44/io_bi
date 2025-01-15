import requests
from datetime import datetime, timedelta, date
from dateutil.parser import parse
import time
import requests
import json
import pandas as pd
import os
from openpyxl import load_workbook
import getpass
import streamlit as st
import numpy as np
import matplotlib.pyplot as plt


access_token = None

def datetime_map(date_str):
    if date_str:
        try:
            # Use dateutil to handle different fractional second lengths automatically
            parsed_date = parse(date_str)
            return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"Error parsing date: {date_str}, {e}")
            return None
    return None

def check_and_get_token(username,password):
    global access_token, expiration_time

    if access_token is None or expiration_time is None or datetime.now() >= expiration_time:
        print("Token expired or not available. Fetching a new one...")
        return get_token(username,password)
    else:
        print(f"Token is still valid. Expires on: {expiration_time}")
        return access_token

def get_token(username,password):
    global access_token, expiration_time

    url = 'https://myapi.logiwa.com/v3.1/Authorize/token'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    payload = {
        "email": f'{username}',
        "password": f'{password}'
    }

    try:
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            json_response = response.json()
            access_token = json_response.get('token')

            expiration_time = datetime.now() + timedelta(days=30)

            print(f"New token generated. Token: {access_token}, Expires on: {expiration_time}")
            return access_token
        else:
            print(f"Failed to retrieve token. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error while retrieving token: {str(e)}")
        return None
    
def lookup_clients(access_token):
    lookup_url = "https://myapi.logiwa.com/v3.1/Helper/lookup"
    lookup_headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    try:
        response = requests.get(lookup_url, headers=lookup_headers)

        if response.status_code == 200:
            response_data = response.json()
            client_list = response_data.get('data', {}).get('clientList', [])
            client_dict = {client['name']: client['identifier'] for client in client_list}
            client_dict["All"] = "All"

            
            return client_dict
        else:
            print(f"Failed to retrieve clients. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error while retrieving clients: {str(e)}")
        return None

def fetch_and_post_data(start_date,end_date,client_identifier, access_token):
    #access_token = st.session_state.access_token
    #client_identifier = 'All'
    now = datetime.utcnow()
    date2 = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    date1 = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    page_index = 0
    page_size = 200
    all_orders = []
    all_job_codes = []
    all_data = []

    while True:
        if client_identifier == "All":
            api_url = f'https://myapi.logiwa.com/v3.1/ShipmentOrder/list/i/{page_index}/s/{page_size}?Status.eq=20&ActualShipmentDate.bt={date1},{date2}'
        else:
            api_url = f'https://myapi.logiwa.com/v3.1/ShipmentOrder/list/i/{page_index}/s/{page_size}?ClientIdentifier.eq={client_identifier}&Status.eq=20&ActualShipmentDate.bt={date1},{date2}'

        request_headers = {
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.get(api_url, headers=request_headers)
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Content: {response.text}")  # Log the full response for debugging

        try:
            response_data = response.json().get("data")
        except requests.exceptions.JSONDecodeError as e:
            print(f"JSON Decode Error: {e}")
            st.error(f"Failed to decode JSON: {e}")
            return [], [], []  # Return empty results to prevent the script from crashing

        #response_data = response.json().get("data")
        records_fetched = response.json().get("totalCount", 0)
        data_length = len(response_data)

        if records_fetched != 0:
            for data in response_data:
                job_code = data.get('warehouseJobCode', '')
                shiporder_code = data.get('code', '')

                if job_code not in all_job_codes:
                    all_job_codes.append(job_code)

                if shiporder_code not in all_orders:
                    all_orders.append(shiporder_code)

                for info in data['shipmentInfo']:
                    so_date = datetime_map(data.get('shipmentOrderDate', ''))
                    so_shipped_date = datetime_map(data.get('actualShipmentDate', ''))
                    custom_dttm_1 = datetime_map(data.get('customFieldDateTime1', ''))
                    custom_dttm_2 = datetime_map(data.get('customFieldDateTime2', ''))
                    custom_dttm_3 = datetime_map(data.get('customFieldDateTime3', ''))

                    row_data = (
                        data['clientDisplayName'], data['channelOrderNumber'], data['code'], data['totalQuantity'], data['shipmentOrderTypeName'],
                        data['shipmentOrderStatusName'], so_date, so_shipped_date, data.get('extraNote1', ''), data.get('extraNote2', ''),
                        info['productSku'], info['productName'], info['packTypeName'], info['packQuantity'],
                        info['uomQuantity'], info['licensePlateNumber'], info['licensePlateTypeCode'],
                        info['trackingNumber'], data.get('carrierName', ''), data.get('shippingOptionName', ''),
                        custom_dttm_1, custom_dttm_2, custom_dttm_3, data.get('customFieldToggle1', ''), data.get('customFieldToggle2', ''),
                        data.get('customFieldDropDown1', ''), data.get('customFieldDropDown2', ''),
                        data.get('customFieldTextBox1', ''), data.get('customFieldTextBox2', ''), data.get('customFieldTextBox3', ''),
                        job_code
                    )
                    all_data.append(row_data)
            page_index += 1
            print(page_index)            
        else:
            break
        if data_length < page_size:
            break
    unique_orders = list(set(all_orders))
    unique_job_codes = list(set(all_job_codes))
    return unique_job_codes, unique_orders, all_data

def fetch_transaction_history(unique_job_codes, unique_orders, access_token):
    #import requests
    #import json
    #from datetime import datetime
    #global access_token
    warehouse_identifier = '3cd7c232-1dcd-4ff3-8eb4-fd43ce1060c4'  # eShipper
    page_size = 200
    all_transactions = []

    results_by_order = {}

    print("Loop", unique_job_codes)
    print("Loop", unique_orders)

    for job_code in unique_job_codes:
        page_index = 0
        total_count = None

        while True:
            url = f'https://myapi.logiwa.com/v3.1/Report/WarehouseTask/i/{page_index}/s/{page_size}?WarehouseIdentifier.eq={warehouse_identifier}&TaskType.in=1,6&TaskStatus.in=3&JobCode.eq={job_code}'
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }

            response = requests.get(url, headers=headers)
            try:
                json_response = response.json()
            except json.JSONDecodeError as err:
                print(f"Error decoding JSON: {err}")
                break

            if total_count is None:
                total_count = json_response.get('totalCount', 0)

            current_page_data = json_response.get('data', [])
            if not current_page_data:
                print(f"No data found for job code {job_code}. Moving to next.")
                break

            for task in current_page_data:
                shipment_order_code = task.get('shipmentOrderCode')

                if not shipment_order_code or shipment_order_code not in unique_orders:
                    continue

                if shipment_order_code not in results_by_order:
                    results_by_order[shipment_order_code] = {'picking': [], 'packing': []}

                if task['warehouseTaskTypeName'] == 'Picking':
                    results_by_order[shipment_order_code]['picking'].append(task)
                elif task['warehouseTaskTypeName'] == 'Packing':
                    results_by_order[shipment_order_code]['packing'].append(task)

            page_index += 1
            print(f"Fetched page {page_index} for job_code {job_code}")

            if page_index * page_size >= total_count:
                break

    for shipment_order_code, tasks in results_by_order.items():
        picking_tasks = tasks['picking']
        packing_tasks = tasks['packing']

        try:
            picking_users = list({task.get('executedByName', 'Unknown') for task in picking_tasks})
        except Exception as e:
            print(f"Error processing picking users for {shipment_order_code}: {e}")
            picking_users = []

        try:
            packing_users = list({task.get('executedByName', 'Unknown') for task in packing_tasks})
        except Exception as e:
            print(f"Error processing packing users for {shipment_order_code}: {e}")
            packing_users = []

        try:
            picking_started = min((datetime_map(task['actualStartDateTime']) for task in picking_tasks), default=None)
        except Exception as e:
            print(f"Error processing picking started time for {shipment_order_code}: {e}")
            picking_started = None

        try:
            picking_finished = max((datetime_map(task['actualFinishDateTime']) for task in picking_tasks), default=None)
        except Exception as e:
            print(f"Error processing picking finished time for {shipment_order_code}: {e}")
            picking_finished = None

        try:
            packing_finished = max((datetime_map(task['actualFinishDateTime']) for task in packing_tasks), default=None)
        except Exception as e:
            print(f"Error processing packing finished time for {shipment_order_code}: {e}")
            packing_finished = None

        try:
            client_name = (picking_tasks[0].get('clientDisplayName') if picking_tasks
                           else packing_tasks[0].get('clientDisplayName', 'Unknown'))
        except Exception as e:
            print(f"Error processing client name for {shipment_order_code}: {e}")
            client_name = 'Unknown'

        row = (
            client_name,
            shipment_order_code,
            picking_started if picking_started else None,
            picking_finished if picking_finished else None,
            ', '.join(picking_users),
            None,  
            packing_finished if packing_finished else None,
            ', '.join(packing_users)
        )
        all_transactions.append(row)

    print(f"Processed {len(all_transactions)} transactions in total")
    return all_transactions


def prompt_for_credentials():
    st.subheader("Step 1: Enter Credentials")
    username = st.text_input("Username", placeholder="Enter your username")
    password = st.text_input("Password", type="password", placeholder="Enter your password")
    if st.button("Submit Credentials"):
        if username and password:
            return username, password
        else:
            st.error("Please enter both username and password!")
    return None, None

def prompt_for_dates():
    st.subheader("Step 2: Select Dates")
    start_date = st.date_input("Start Date", value=date.today())
    end_date = st.date_input("End Date", value=date.today())
    if st.button("Submit Dates"):
        if start_date > end_date:
            st.error("End date must be after the start date!")
        else:
            return start_date, end_date
    return None, None

def prompt_for_client(client_dict):
    st.subheader("Step 3: Select Client")
    selected_client = st.selectbox("Client", options=["Select a Client"] + list(client_dict.keys()))
    if st.button("Submit Client"):
        if selected_client != "Select a Client":
            return selected_client, client_dict.get(selected_client)
        else:
            st.error("Please select a valid client!")
    return None, None

def main():
    st.title("Visualization Tool")
    
    # Initialize session state variables
    if "credentials_entered" not in st.session_state:
        st.session_state.credentials_entered = False
    if "dates_selected" not in st.session_state:
        st.session_state.dates_selected = False
    if "client_selected" not in st.session_state:
        st.session_state.client_selected = False
    if "username" not in st.session_state:
        st.session_state.username = None
    if "password" not in st.session_state:
        st.session_state.password = None
    if "access_token" not in st.session_state:
        st.session_state.access_token = None
    if "start_date" not in st.session_state:
        st.session_state.start_date = None
    if "end_date" not in st.session_state:
        st.session_state.end_date = None
    if "selected_client" not in st.session_state:
        st.session_state.selected_client = None
    if "client_identifier" not in st.session_state:
        st.session_state.client_identifier = None

    placeholder = st.empty()
    client_dict = lookup_clients(st.session_state.access_token)

    # Step 1: Prompt for credentials
    if not st.session_state.credentials_entered:
        with placeholder.container():
            username, password = prompt_for_credentials()
            if username and password:
                st.session_state.username = username
                st.session_state.password = password
                st.session_state.credentials_entered = True
                st.success("Credentials accepted. Proceeding...")
            else:
                st.stop()  # Stop execution until valid input is provided

    # Step 2: Generate access token (dummy function here for example)
    if not st.session_state.access_token:
        st.session_state.access_token = get_token(
            st.session_state.username, st.session_state.password
        )
        st.write("Access token generated.")

    # Step 3: Prompt for dates
    if not st.session_state.dates_selected:
        with placeholder.container():
            start_date, end_date = prompt_for_dates()
            if start_date and end_date:
                st.session_state.start_date = start_date
                st.session_state.end_date = end_date
                st.session_state.dates_selected = True
                st.success(f"Dates selected: {start_date} to {end_date}")
            else:
                st.stop()  # Stop execution until valid input is provided


    # Step 4: Lookup clients and prompt for client selection
    
    if not st.session_state.client_selected:
        with placeholder.container():
            selected_client, client_identifier = prompt_for_client(client_dict)
            if selected_client and client_identifier:
                st.session_state.selected_client = selected_client
                st.session_state.client_identifier = client_identifier
                st.session_state.client_selected = True
                st.success(f"Client selected: {selected_client}")
            else:
                st.stop()  # Stop execution until valid input is provided

    # Step 5: Final validation
    if st.session_state.selected_client == "All":
        date_difference = (
            st.session_state.end_date - st.session_state.start_date
        ).days
        if date_difference > 120:
            st.error("Error: When 'All' clients are selected, the date range cannot be more than 4 months.")
            st.stop()

    st.write("All inputs collected successfully. Proceeding with visualization...")

    unique_job_codes, unique_orders, all_data = fetch_and_post_data(
        st.session_state.start_date, 
        st.session_state.end_date,
        st.session_state.client_identifier,
        st.session_state.access_token)
    
    all_transactions1 = fetch_transaction_history(unique_job_codes,unique_orders,st.session_state.access_token)
    
    column_headers_1 = [
        'Client Name', 'Channel Order Number', 'Order Code', 'Total Quantity', 'Order Type',
        'Order Status', 'Shipment Order Date', 'Actual Shipment Date', 'Extra Note 1', 'Extra Note 2',
        'Product SKU', 'Product Name', 'Pack Type', 'Pack Quantity', 'UOM Quantity', 'License Plate Number',
        'License Plate Type Code', 'Tracking Number', 'Carrier Name', 'Shipping Option Name',
        'Custom DateTime 1', 'Custom DateTime 2', 'Custom DateTime 3', 'Custom Toggle 1', 'Custom Toggle 2',
        'Custom Dropdown 1', 'Custom Dropdown 2', 'Custom TextBox 1', 'Custom TextBox 2', 'Custom TextBox 3',
        'Job Code'
    ]
    column_headers_2 = [
        'Client Name', 'Order Code', 'Picking Started', 'Picking Finished', 'Picking User',
        'Packing Started', 'Packing Finished', 'Packing User'
    ]
    
    df_fetch_data = pd.DataFrame(all_data, columns=column_headers_1)
    df_transaction_history = pd.DataFrame(all_transactions1, columns=column_headers_2)

    # Convert datetime columns for transaction history
    df_transaction_history['Picking Started'] = pd.to_datetime(df_transaction_history['Picking Started'])
    df_transaction_history['Picking Finished'] = pd.to_datetime(df_transaction_history['Picking Finished'])
    df_transaction_history['Packing Started'] = pd.to_datetime(df_transaction_history['Packing Started'])
    df_transaction_history['Packing Finished'] = pd.to_datetime(df_transaction_history['Packing Finished'])

    # Convert datetime columns for fetch data
    df_fetch_data['Shipment Order Date'] = pd.to_datetime(df_fetch_data['Shipment Order Date'])
    df_fetch_data['Actual Shipment Date'] = pd.to_datetime(df_fetch_data['Actual Shipment Date'])

    # Calculate durations for transaction history
    df_transaction_history['Picking Duration'] = (df_transaction_history['Picking Finished'] - df_transaction_history['Picking Started']).dt.total_seconds() / 60
    df_transaction_history['Packing Duration'] = (df_transaction_history['Packing Finished'] - df_transaction_history['Packing Started']).dt.total_seconds() / 60
    df_transaction_history['Total Duration (Picking to Packing)'] = (df_transaction_history['Packing Finished'] - df_transaction_history['Picking Started']).dt.total_seconds() / 60

        
    # Group by Client Name and calculate averages
    df_grouped_transaction = df_transaction_history.groupby('Client Name').agg({
        'Picking Duration': 'mean',
        'Packing Duration': 'mean',
        'Total Duration (Picking to Packing)': 'mean'
    }).reset_index()

    tab1, tab2 = st.tabs(["Transaction History Analytics", "Fetch Data Analytics"])


    with tab1:
        st.header("Transaction History Analytics")
        
        # Display grouped data
        st.subheader("Average Durations by Client")
        st.dataframe(df_grouped_transaction)

        # Visualize Picking Duration
        st.subheader("Picking Duration by Client")
        fig1, ax1 = plt.subplots()
        df_grouped_transaction.plot(x='Client Name', y='Picking Duration', kind='bar', ax=ax1, legend=False)
        ax1.set_title("Average Picking Duration (Minutes)")
        ax1.set_xlabel("Client Name")
        ax1.set_ylabel("Duration (Minutes)")
        st.pyplot(fig1)

        # Visualize Packing Duration
        st.subheader("Packing Duration by Client")
        fig2, ax2 = plt.subplots()
        df_grouped_transaction.plot(x='Client Name', y='Packing Duration', kind='bar', ax=ax2, legend=False)
        ax2.set_title("Average Packing Duration (Minutes)")
        ax2.set_xlabel("Client Name")
        ax2.set_ylabel("Duration (Minutes)")
        st.pyplot(fig2)

        # Visualize Total Duration (Picking to Packing)
        st.subheader("Total Duration (Picking to Packing) by Client")
        fig3, ax3 = plt.subplots()
        df_grouped_transaction.plot(x='Client Name', y='Total Duration (Picking to Packing)', kind='bar', ax=ax3, legend=False)
        ax3.set_title("Average Total Duration (Minutes)")
        ax3.set_xlabel("Client Name")
        ax3.set_ylabel("Duration (Minutes)")
        st.pyplot(fig3)

    with tab2:
        st.header("Fetch Data Analytics")

        # Calculate analytics for fetch data
        df_grouped_fetch = df_fetch_data.groupby('Client Name').agg({
            'Total Quantity': 'mean',  # Average Total Quantity
        }).reset_index()

        df_grouped_fetch['Avg Shipment Difference (Days)'] = df_fetch_data.groupby('Client Name').apply(
            lambda x: (x['Actual Shipment Date'] - x['Shipment Order Date']).dt.days.mean()
        ).values

        # Most common carriers and shipping options
        most_common_carriers = df_fetch_data['Carrier Name'].value_counts().head(5)
        most_common_shipping_options = df_fetch_data['Shipping Option Name'].value_counts().head(5)

        # Display grouped data
        st.subheader("Average Total Quantity and Shipment Difference by Client")
        st.dataframe(df_grouped_fetch)

        # Visualize Average Total Quantity
        st.subheader("Average Total Quantity by Client")
        fig4, ax4 = plt.subplots()
        df_grouped_fetch.plot(x='Client Name', y='Total Quantity', kind='bar', ax=ax4, legend=False)
        ax4.set_title("Average Total Quantity by Client")
        ax4.set_xlabel("Client Name")
        ax4.set_ylabel("Total Quantity")
        st.pyplot(fig4)

        # Visualize Average Shipment Difference
        st.subheader("Average Shipment Difference by Client")
        fig5, ax5 = plt.subplots()
        df_grouped_fetch.plot(x='Client Name', y='Avg Shipment Difference (Days)', kind='bar', ax=ax5, legend=False)
        ax5.set_title("Average Shipment Difference by Client")
        ax5.set_xlabel("Client Name")
        ax5.set_ylabel("Shipment Difference (Days)")
        st.pyplot(fig5)

        # Display most common carriers and shipping options
        st.subheader("Most Common Carriers")
        st.bar_chart(most_common_carriers)

        st.subheader("Most Common Shipping Options")
        st.bar_chart(most_common_shipping_options)






if __name__ == "__main__":
    main()


