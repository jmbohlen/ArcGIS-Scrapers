import io
import os
import zipfile
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import datetime as dt

RTPD_12_lmp_node_list = []
RTPD_12_energy_node_list = []
RTPD_12_losses_node_list = []
RTPD_12_congestion_node_list = []
RTPD_12_green_node_list = []
RTPD_34_lmp_node_list = []
RTPD_34_energy_node_list = []
RTPD_34_losses_node_list = []
RTPD_34_congestion_node_list = []
RTPD_34_green_node_list = []
RTPD1_energy_list = []
RTPD1_lmp_list = []
RTPD1_losses_list = []
RTPD1_cong_list = []
RTPD1_green_list = []
RTPD1_lmp_int_list = []
RTPD2_energy_list = []
RTPD2_lmp_list = []
RTPD2_losses_list = []
RTPD2_cong_list = []
RTPD2_green_list = []
RTPD2_lmp_int_list = []
RTPD3_energy_list = []
RTPD3_lmp_list = []
RTPD3_losses_list = []
RTPD3_cong_list = []
RTPD3_green_list = []
RTPD3_lmp_int_list = []
RTPD4_energy_list = []
RTPD4_lmp_list = []
RTPD4_losses_list = []
RTPD4_cong_list = []
RTPD4_green_list = []
RTPD4_lmp_int_list = []

RTPD_data_df = pd.read_csv('RTPD_Prices.csv', index_col=0)
# RTPD
# Parse for first two intervals if current time is between XX40-XX50
if int(dt.datetime.utcnow().strftime("%M")) >= 47:
    date1 = (dt.datetime.utcnow() + dt.timedelta(hours=1)).strftime("%Y%m%d")
    time1 = (dt.datetime.utcnow() + dt.timedelta(hours=1)).strftime("%H")

    RTPDurl = 'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_RTPD_LMP&startdatetime=' + date1 + 'T' + time1 + ':00-0000&enddatetime=' + date1 + 'T' + time1 + ':00-0000&version=1&market_run_id=RTPD&grp_type=ALL'
    r = requests.get(RTPDurl)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()

    file_name = z.namelist()
    tree = ET.parse(file_name[0])
    root = tree.getroot()
    node_data = root[1][0]
    for node in node_data[1:-1]:
        data_item = node[1][0].text
        if data_item == 'LMP_PRC':
            RTPD_12_lmp_node_list.append(node[1][1].text)
            RTPD1_lmp_int_list.append(node[1][5].text[11:16])
            RTPD1_lmp_list.append(node[1][6].text)
            RTPD2_lmp_int_list.append(node[2][5].text[11:16])
            RTPD2_lmp_list.append(node[2][6].text)
        if data_item == 'LMP_CONG_PRC':
            RTPD_12_congestion_node_list.append(node[1][1].text)
            RTPD1_cong_list.append(node[1][6].text)
            RTPD2_cong_list.append(node[2][6].text)
        if data_item == 'LMP_ENE_PRC':
            RTPD_12_energy_node_list.append(node[1][1].text)
            RTPD1_energy_list.append(node[1][6].text)
            RTPD2_energy_list.append(node[2][6].text)
        if data_item == 'LMP_LOSS_PRC':
            RTPD_12_losses_node_list.append(node[1][1].text)
            RTPD1_losses_list.append(node[1][6].text)
            RTPD2_losses_list.append(node[2][6].text)

    lmp_12_df = pd.DataFrame({'node': RTPD_12_lmp_node_list,
                              'RTPD1_lmp': RTPD1_lmp_list,
                              'RTPD2_lmp': RTPD2_lmp_list,
                              'RTPD1_int_end': RTPD1_lmp_int_list,
                              'RTPD2_int_end': RTPD2_lmp_int_list,
                              })
    energy_12_df = pd.DataFrame({'node': RTPD_12_energy_node_list,
                                 'RTPD1_energy': RTPD1_energy_list,
                                 'RTPD2_energy': RTPD2_energy_list
                                 })
    cong_12_df = pd.DataFrame({'node': RTPD_12_congestion_node_list,
                               'RTPD1_congestion': RTPD1_cong_list,
                               'RTPD2_congestion': RTPD2_cong_list
                               })
    loss_12_df = pd.DataFrame({'node': RTPD_12_losses_node_list,
                               'RTPD1_losses': RTPD1_losses_list,
                               'RTPD2_losses': RTPD2_losses_list
                               })
    RTPD_12_combined = pd.merge(lmp_12_df, energy_12_df, how='left', left_on='node', right_on='node')
    RTPD_12_combined = pd.merge(RTPD_12_combined, cong_12_df, how='left', left_on='node', right_on='node')
    RTPD_12_combined = pd.merge(RTPD_12_combined, loss_12_df, how='left', left_on='node', right_on='node')

    cols_to_remove = []
    for x in RTPD_data_df.columns:
        if x.startswith(('RTPD1', 'RTPD2')):
            cols_to_remove.append(x)

    RTPD_data_df = RTPD_data_df.drop(cols_to_remove, axis=1)

    RTPD_updated_df = pd.merge(RTPD_data_df, RTPD_12_combined, how='left', left_on='node', right_on='node', suffixes=('', ''))
    p1_level = []
    p2_level = []
    for index, row in RTPD_updated_df.iterrows():
        p1 = row['RTPD1_lmp']
        p2 = row['RTPD2_lmp']
        if float(p1) <= -50:
            p1_level.append(1)
        elif -50 < float(p1) <= 0:
            p1_level.append(2)
        elif 0 < float(p1) <= 50:
            p1_level.append(3)
        elif 50 < float(p1) <= 55:
            p1_level.append(4)
        elif 55 < float(p1) <= 60:
            p1_level.append(5)
        elif 60 < float(p1) <= 65:
            p1_level.append(6)
        elif 65 < float(p1) <= 70:
            p1_level.append(7)
        elif 70 < float(p1) <= 75:
            p1_level.append(8)
        elif 75 < float(p1) <= 80:
            p1_level.append(9)
        elif 80 < float(p1) <= 85:
            p1_level.append(10)
        elif 85 < float(p1) <= 90:
            p1_level.append(11)
        elif 90 < float(p1) <= 95:
            p1_level.append(12)
        elif 95 < float(p1) <= 100:
            p1_level.append(13)
        elif 100 < float(p1) <= 125:
            p1_level.append(14)
        elif 125 < float(p1) <= 150:
            p1_level.append(15)
        elif 150 < float(p1) <= 200:
            p1_level.append(16)
        elif 200 < float(p1):
            p1_level.append(17)
        else:
            p1_level.append(float("NaN"))
        if float(p2) <= -50:
            p2_level.append(1)
        elif -50 < float(p2) <= 0:
            p2_level.append(2)
        elif 0 < float(p2) <= 50:
            p2_level.append(3)
        elif 50 < float(p2) <= 55:
            p2_level.append(4)
        elif 55 < float(p2) <= 60:
            p2_level.append(5)
        elif 60 < float(p2) <= 65:
            p2_level.append(6)
        elif 65 < float(p2) <= 70:
            p2_level.append(7)
        elif 70 < float(p2) <= 75:
            p2_level.append(8)
        elif 75 < float(p2) <= 80:
            p2_level.append(9)
        elif 80 < float(p2) <= 85:
            p2_level.append(10)
        elif 85 < float(p2) <= 90:
            p2_level.append(11)
        elif 90 < float(p2) <= 95:
            p2_level.append(12)
        elif 95 < float(p2) <= 100:
            p2_level.append(13)
        elif 100 < float(p2) <= 125:
            p2_level.append(14)
        elif 125 < float(p2) <= 150:
            p2_level.append(15)
        elif 150 < float(p2) <= 200:
            p2_level.append(16)
        elif 200 < float(p2):
            p2_level.append(17)
        else:
            p2_level.append(float("NaN"))
    RTPD_updated_df['p1_level'] = p1_level
    RTPD_updated_df['p2_level'] = p2_level

    RTPD_updated_df.to_csv('RTPD_Prices.csv')

    try:
        os.remove(file_name[0])
    except OSError:
        pass
    print("Successfully got Interval 1+2 data from OASIS")

# Parse for second two intervals if current time is between XX10-XX20
if 15 < int(dt.datetime.utcnow().strftime("%M")) < 47:
    date1 = dt.datetime.utcnow().strftime("%Y%m%d")
    time1 = dt.datetime.utcnow().strftime("%H")

    RTPDurl = 'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_RTPD_LMP&startdatetime=' + date1 + 'T' + time1 + ':00-0000&enddatetime=' + date1 + 'T' + time1 + ':00-0000&version=1&market_run_id=RTPD&grp_type=ALL'
    r = requests.get(RTPDurl)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()

    file_name = z.namelist()
    tree = ET.parse(file_name[0])
    root = tree.getroot()
    node_data = root[1][0]

    for node in node_data[1:-1]:
        data_item = node[1][0].text
        if len(node) == 5:
            if data_item == 'LMP_PRC':
                RTPD_34_lmp_node_list.append(node[1][1].text)
                RTPD3_lmp_int_list.append(node[3][5].text[11:16])
                RTPD3_lmp_list.append(node[3][6].text)
                RTPD4_lmp_int_list.append(node[4][5].text[11:16])
                RTPD4_lmp_list.append(node[4][6].text)
            if data_item == 'LMP_CONG_PRC':
                RTPD_34_congestion_node_list.append(node[1][1].text)
                RTPD3_cong_list.append(node[3][6].text)
                RTPD4_cong_list.append(node[4][6].text)
            if data_item == 'LMP_ENE_PRC':
                RTPD_34_energy_node_list.append(node[1][1].text)
                RTPD3_energy_list.append(node[3][6].text)
                RTPD4_energy_list.append(node[4][6].text)
            if data_item == 'LMP_LOSS_PRC':
                RTPD_34_losses_node_list.append(node[1][1].text)
                RTPD3_losses_list.append(node[3][6].text)
                RTPD4_losses_list.append(node[4][6].text)

    lmp_34_df = pd.DataFrame({'node': RTPD_34_lmp_node_list,
                              'RTPD3_lmp': RTPD3_lmp_list,
                              'RTPD4_lmp': RTPD4_lmp_list,
                              'RTPD3_int_end': RTPD3_lmp_int_list,
                              'RTPD4_int_end': RTPD4_lmp_int_list,
                              })
    energy_34_df = pd.DataFrame({'node': RTPD_34_energy_node_list,
                                 'RTPD3_energy': RTPD3_energy_list,
                                 'RTPD4_energy': RTPD4_energy_list
                                 })
    cong_34_df = pd.DataFrame({'node': RTPD_34_congestion_node_list,
                               'RTPD3_congestion': RTPD3_cong_list,
                               'RTPD4_congestion': RTPD4_cong_list
                               })
    loss_34_df = pd.DataFrame({'node': RTPD_34_losses_node_list,
                               'RTPD3_losses': RTPD3_losses_list,
                               'RTPD4_losses': RTPD4_losses_list
                               })
    RTPD_34_combined = pd.merge(lmp_34_df, energy_34_df, how='left', left_on='node', right_on='node')
    RTPD_34_combined = pd.merge(RTPD_34_combined, cong_34_df, how='left', left_on='node', right_on='node')
    RTPD_34_combined = pd.merge(RTPD_34_combined, loss_34_df, how='left', left_on='node', right_on='node')

    cols_to_remove = []
    for x in RTPD_data_df.columns:
        if x.startswith(('RTPD3', 'RTPD4')):
            cols_to_remove.append(x)

    RTPD_data_df = RTPD_data_df.drop(cols_to_remove, axis=1)

    RTPD_updated_df = pd.merge(RTPD_data_df, RTPD_34_combined, how='left', left_on='node', right_on='node')

    p3_level = []
    p4_level = []
    for index, row in RTPD_updated_df.iterrows():
        p3 = row['RTPD3_lmp']
        p4 = row['RTPD4_lmp']
        if float(p3) <= -50:
            p3_level.append(1)
        elif -50 < float(p3) <= 0:
            p3_level.append(2)
        elif 0 < float(p3) <= 50:
            p3_level.append(3)
        elif 50 < float(p3) <= 55:
            p3_level.append(4)
        elif 55 < float(p3) <= 60:
            p3_level.append(5)
        elif 60 < float(p3) <= 65:
            p3_level.append(6)
        elif 65 < float(p3) <= 70:
            p3_level.append(7)
        elif 70 < float(p3) <= 75:
            p3_level.append(8)
        elif 75 < float(p3) <= 80:
            p3_level.append(9)
        elif 80 < float(p3) <= 85:
            p3_level.append(10)
        elif 85 < float(p3) <= 90:
            p3_level.append(11)
        elif 90 < float(p3) <= 95:
            p3_level.append(12)
        elif 95 < float(p3) <= 100:
            p3_level.append(13)
        elif 100 < float(p3) <= 125:
            p3_level.append(14)
        elif 125 < float(p3) <= 150:
            p3_level.append(15)
        elif 150 < float(p3) <= 200:
            p3_level.append(16)
        elif 200 < float(p3):
            p3_level.append(17)
        else:
            p3_level.append(float("NaN"))
        if float(p4) <= -50:
            p4_level.append(1)
        elif -50 < float(p4) <= 0:
            p4_level.append(2)
        elif 0 < float(p4) <= 50:
            p4_level.append(3)
        elif 50 < float(p4) <= 55:
            p4_level.append(4)
        elif 55 < float(p4) <= 60:
            p4_level.append(5)
        elif 60 < float(p4) <= 65:
            p4_level.append(6)
        elif 65 < float(p4) <= 70:
            p4_level.append(7)
        elif 70 < float(p4) <= 75:
            p4_level.append(8)
        elif 75 < float(p4) <= 80:
            p4_level.append(9)
        elif 80 < float(p4) <= 85:
            p4_level.append(10)
        elif 85 < float(p4) <= 90:
            p4_level.append(11)
        elif 90 < float(p4) <= 95:
            p4_level.append(12)
        elif 95 < float(p4) <= 100:
            p4_level.append(13)
        elif 100 < float(p4) <= 125:
            p4_level.append(14)
        elif 125 < float(p4) <= 150:
            p4_level.append(15)
        elif 150 < float(p4) <= 200:
            p4_level.append(16)
        elif 200 < float(p4):
            p4_level.append(17)
        else:
            p4_level.append(float("NaN"))
    RTPD_updated_df['p3_level'] = p3_level
    RTPD_updated_df['p4_level'] = p4_level

    RTPD_updated_df.to_csv('RTPD_Prices.csv')

    try:
        os.remove(file_name[0])
    except OSError:
        pass
    print("Successfully got Interval 3+4 data from OASIS")
