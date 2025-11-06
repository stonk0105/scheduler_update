import pandas as pd
import numpy as np

# class Toolbox:
    # def __init__(self):
        # pass
def AB_N(df):
    AB = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P'])])
    # AB_List=set(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P'])].PA_Result)
    return AB
def PA_N(df):
    PA = len(df[(~df['PA_Result'].isin(['']))&(~df['PA_Result'].isnull())])
    return PA
def AVG(df):
    hit = len(df[df["PA_Result"].isin(['1B', '2B', '3B', 'HR', 'IHR'])])
    AB = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P'])])
    try:
        AVG = '%.3f'%(hit / AB)
    except:
        AVG = '---'
    return AVG
def RISPAVG(df):
    risphit =len(df[(df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR']))&(df['On-Base'].isin([2,3,4,5,6,7]))])
    rispab =len(df[(df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P']))&(df['On-Base'].isin([2,3,4,5,6,7]))])
    try:
        rispba = '%.3f'%(risphit / rispab)
    except:
        rispba = '---'
    return rispba

def OBP(df):
    hit = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR'])])
#保送(四壞球+故意四壞)
    bb = len(df[df['PA_Result'].isin(['BB', 'BB-I', 'BB-IL', 'IBB','BB-P'])])
    hbp=len(df[df['PA_Result'] == 'HBP'])
    sf=len(df[df['PA_Result'].isin(['SF', 'E-SF'])]) 
    AB = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P'])])
    try:
        OBP = '%.3f'%((hit + bb + hbp) / (AB + bb + hbp + sf))
    except:
        OBP = '---'
    return OBP
def SLG(df):
    TB = len(df[df['PA_Result'] == '1B']) * 1 + len(df[df['PA_Result'] == '2B']) * 2 + len(df[df['PA_Result'] == '3B']) * 3 + len(df[df['PA_Result'].isin(['HR', 'IHR'])]) * 4
    AB = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P'])])
    try:
        SLG = '%.3f'%(TB / AB)
    except:
        SLG = '---'
    return SLG
def OPS(df):
    TB = len(df[df['PA_Result'] == '1B']) * 1 + len(df[df['PA_Result'] == '2B']) * 2 + len(df[df['PA_Result'] == '3B']) * 3 + len(df[df['PA_Result'].isin(['HR', 'IHR'])]) * 4
    AB = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P'])])
    try:
        SLG = (TB / AB)
    except:
        SLG = '---'
    hit = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR'])])
#保送(四壞球+故意四壞)
    bb = len(df[df['PA_Result'].isin(['BB', 'BB-I', 'BB-IL', 'IBB','BB-P'])])
    hbp=len(df[df['PA_Result'] == 'HBP'])
    sf=len(df[df['PA_Result'].isin(['SF', 'E-SF'])]) 
    AB = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P'])])
    try:
        OBP = ((hit + bb + hbp) / (AB + bb + hbp + sf))
    except:
        OBP = '---'
    if OBP == '---' and SLG == '---':
        OPS = '---'
    elif OBP == '---':
        OPS = '%.3f'%(SLG)
    elif SLG == '---':
        OPS = '%.3f'%(OBP)
    else:
        OPS = '%.3f'%(round(OBP,3) + round(SLG,3))
    return OPS

def Swing_P(df):
    pitch = len(df[df['PitchCode'].isin(['Strk-S', 'Strk-C', 'Foul', 'In-Play', 'Ball','Ball-B-I'])])
    swingC = len(df[df['PitchCode'].isin(['Strk-S','Foul','In-Play'])])
    try:
        swing = '%.1f'%((swingC * 100) / pitch)
    except:
        swing = '---'
    return swing
def Z_Swing_P(df):
    zone = len(df[df['Zone'] == 1])
    zswingC = len(df[(df['PitchCode'].isin(['Strk-S', 'Foul', 'In-Play'])) & (df['Zone'] == 1)])
    try:
        Zswing = '%.1f'%((zswingC * 100) / zone)
    except:
        Zswing = '---'
    return Zswing
def O_Swing_P(df):
    ozone = len(df[df['Zone'] == 0])
    oswingC = len(df[(df['PitchCode'].isin(['Strk-S', 'Foul', 'In-Play'])) & (df['Zone'] == 0)])
    try:
        Oswing = '%.1f'%((oswingC * 100) / ozone)
    except:
        Oswing = '---'
    return Oswing
def Whiff_P(df):
    miss = len(df[df['PitchCode'] == 'Strk-S'])
    swingC = len(df[df['PitchCode'].isin(['Strk-S','Foul','In-Play'])])
    try:
        whiff = '%.1f'%((miss * 100) / swingC)
    except:
        whiff = '---'
    return whiff    

def K_P(df):
    k = len(df[df['PA_Result'].isin(['K', 'Ks', 'K-DO', 'K-BS', 'K-BF', 'K-DS','K-SF','K-P'])])
    PA = len(df[(~df['PA_Result'].isin(['']))&(~df['PA_Result'].isnull())])
    try:
        k_p = '%.1f'%((k * 100) / PA)
    except:
        k_p = '---'
    return k_p
def BB_P(df):
    bb = len(df[df['PA_Result'].isin(['BB', 'BB-I', 'BB-IL', 'IBB', 'HBP','BB-P'])])
    PA = len(df[~df['PA_Result'].isin([''])&(~df['PA_Result'].isnull())])
    try:
        bb_p = '%.1f'%((bb) * 100 / PA)
    except:
        bb_p = '---'
    return bb_p
def GB_P(df):
    GB = len(df[df['HitType'] == 'GROUND'])
    inplay = len(df[df['PitchCode'] == 'In-Play'])
    try:
        GB_P = '%.1f'%((GB * 100) / inplay)
    except:
        GB_P = '---'
    return GB_P
def LD_P(df):
    LD = len(df[df['HitType'] == 'LINE'])
    inplay = len(df[df['PitchCode'] == 'In-Play'])
    try:
        LD_P = '%.1f'%((LD * 100) / inplay)
    except:
        LD_P = '---'
    return LD_P
def FB_P(df):
    FB = len(df[df['HitType'].isin(['FLYB','POPB'])])
    inplay = len(df[df['PitchCode'] == 'In-Play'])
    try:
        FB_P = '%.1f'%((FB * 100) / inplay)
    except:
        FB_P = '---'
    return FB_P
def IFFB_P(df):
    POP = len(df[df['HitType'] == 'POPB'])
    FB = len(df[df['HitType'].isin(['FLYB','POPB'])])
    try:
        IFFB = '%.1f'%((POP * 100) / FB )
    except:
        IFFB = '---'
    return IFFB
def GB_FB(df):
    GB = len(df[df['HitType'] == 'GROUND'])
    FB = len(df[df['HitType'].isin(['FLYB','POPB'])])
    try:
        gb_fb = '%.2f'%(GB / FB)
    except:
        gb_fb = '---'
    return gb_fb
def Zone_P(df):
    zone = len(df[df['Zone'] == 1])
    pitch = len(df[df['PitchCode'].isin(['Strk-S', 'Strk-C', 'Foul', 'In-Play', 'Ball'])])
    try:
        Zone = '%.1f'%(zone * 100 / pitch)
    except:
        Zone = '---'
    return Zone
def O_Zone_P(df):
    ozone = len(df[df['Zone'] == 0])
    pitch = len(df[df['PitchCode'].isin(['Strk-S', 'Strk-C', 'Foul', 'In-Play', 'Ball','Ball-B-I'])])
    try:
        OZone = '%.1f'%(ozone * 100 / pitch)
    except:
        OZone = '---'
    return OZone
def Strike_P(df):
    strike = len(df[df['PitchCode'].isin(['Strk-S', 'Strk-C', 'Foul', 'In-Play'])])
    pitch = len(df[df['PitchCode'].isin(['Strk-S', 'Strk-C', 'Foul', 'In-Play', 'Ball','Ball-B-I'])])
    try:
        Strike = '%.1f'%((strike *100) / pitch)
    except:
        Strike = '---'
    return Strike
def BABIP(df):
    Sf = len(df[df['PA_Result'] == 'SF'])
    Hr = len(df[df['PA_Result'].isin(['HR','IHR'])])
    Ab_BABIP = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P'])])
    So = len(df[df['PA_Result'].isin(['K','K-BF','K-BS','K-DO','K-DS','K-SF','Ks','K-P'])])
    Hit = len(df[df['PA_Result'].isin(['1B','2B','3B','HR','IHR'])])
    try:
            BABIp = '%.3f'%((Hit - Hr) / (Ab_BABIP - So - Hr + Sf))
    except:
            BABIp='---'
    return BABIp
def Contact_P(df):
    contact = len(df[df['PitchCode'].isin(['Foul','In-Play'])])
    swingC = len(df[df['PitchCode'].isin(['Strk-S','Foul','In-Play'])])
    try:
        Contact = '%.1f'%((contact * 100) / swingC)
    except:
        Contact = '---'
    return Contact
def Z_Contact_P(df):
    zcontact = len(df[(df['PitchCode'].isin(['Foul','In-Play'])) & (df['Zone'] == 1)])
    zswingC = len(df[(df['PitchCode'].isin(['Strk-S','Foul','In-Play']) & (df['Zone'] == 1))])
    try:
        Z_Contact = '%.1f'%((zcontact * 100) / zswingC)
    except:
        Z_Contact = '---'
    return Z_Contact
def O_Contact_P(df):
    ocontact = len(df[(df['PitchCode'].isin(['Foul','In-Play'])) & (df['Zone'] == 0)])
    oswingC = len(df[(df['PitchCode'].isin(['Strk-S','Foul','In-Play']) & (df['Zone'] == 0))])
    try:
        O_Contact = '%.1f'%((ocontact * 100) / oswingC)
    except:
        O_Contact = '---'
    return O_Contact
def CSW(df):
    Pitches = len(df)
    Strike_CSW = len(df[df['PitchCode'].isin(['Strk-S','Strk-C'])])
    try:
            CSW=str('%.1f'%(Strike_CSW * 100 / Pitches))
    except:
            CSW='---'
    return CSW
def ISO(df):
#SLG - AVG 化簡後的算法
    Ab = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P'])])
    N_2b = len(df[df['PA_Result'].isin(['2B'])])
    N_3b = len(df[df['PA_Result'].isin(['3B'])])
    N_Hr = len(df[df['PA_Result'].isin(['HR', 'IHR'])])
    try:
        ISO = '%.3f'%((N_2b + N_3b * 2 + N_Hr * 3) / (Ab))
    except:
        ISO = '---'
    return ISO
def Soft_P(df):
    soft = df[df['HardnessTag'].isin(['SOFT'])]
    inplay = df[df['HardnessTag'].isin(['HARD','MED','SOFT'])]
    try:
        Soft_P = '%.1f'%((len(soft) * 100) / (len(inplay)))
    except:
        Soft_P = '---'
    return Soft_P

def Med_P(df):
    Med = df[df['HardnessTag'].isin(['MED'])]
    inplay = df[df['HardnessTag'].isin(['HARD','MED','SOFT'])]
    try:
        Med_P = '%.1f'%((len(Med) * 100) / (len(inplay)))
    except:
        Med_P = '---'
    return Med_P
def Hard_P(df):
    Hard = df[df['HardnessTag'].isin(['HARD'])]
    inplay = df[df['HardnessTag'].isin(['HARD','MED','SOFT'])]
    try:
        Hard_P = '%.1f'%((len(Hard) * 100) / (len(inplay)))
    except:
        Hard_P = '---'
    return Hard_P
def Edge_P(df):
    edge = df[~((df['APP_KZoneY'] >= -round(20 / 3, 1)) & (df['APP_KZoneY'] <= round(20 / 3, 1)) & (df['APP_KZoneZ'] <= 38) & (df['APP_KZoneZ'] >= 22))]
    try:
        Edge_P = '%.1f'%((len(edge) * 100) / len(df))
    except:
        Edge_P = '---'
    return Edge_P
def First_Pitch_Swing_P(df):
    firstpitchswing = (df[(df['BS'] == '0-0') & (df['PitchCode'].isin(["Strk-S", "Foul", "In-Play"]))])
    firstpitch  = (df[df['BS'] == '0-0'])
    try:
        First_Pitch_Swing_P = '%.1f'%((len(firstpitchswing) * 100) / len(firstpitch))
    except:
        First_Pitch_Swing_P = '---'
    return First_Pitch_Swing_P
def RISP_First_Pitch_Swing_P(df):  
    firstpitchswing = (df[(df['BS'] == '0-0') & (df['PitchCode'].isin(["Strk-S", "Foul", "In-Play"]))&(df['On-Base'].isin([2,3,4,5,6,7]))])
    firstpitch  = (df[(df['BS'] == '0-0')&(df['On-Base'].isin([2,3,4,5,6,7]))])
    try:
        RISP_First_Pitch_Swing_P = '%.1f'%((len(firstpitchswing) * 100) / len(firstpitch))
    except:
        RISP_First_Pitch_Swing_P = '---'
    return RISP_First_Pitch_Swing_P

def Batter_Lead_Swing_P(df):
    batterleadpitchswing = (df[(df['BS'].isin(["1-0", "2-0", "2-1","3-1","3-0"])) & (df['PitchCode'].isin(["Strk-S", "Foul", "In-Play"]))])
    batterleadpitch  = (df[df['BS'].isin(["1-0", "2-0", "2-1","3-1","3-0"])])
    try:
        Batter_Lead_Swing_P = '%.1f'%((len(batterleadpitchswing) * 100) / len(batterleadpitch))
    except:
        Batter_Lead_Swing_P = '---'
    return Batter_Lead_Swing_P
def Pitcher_Lead_Swing_P(df):
    pitcherpitchswing = (df[(df['BS'].isin(["0-1", "0-2", "1-2"])) & (df['PitchCode'].isin(["Strk-S", "Foul", "In-Play"]))])
    pitcherpitch  = (df[df['BS'].isin(["0-1", "0-2", "1-2"])])
    try:
        Pitcher_Lead_Swing_P = '%.1f'%((len(pitcherpitchswing) * 100) / len(pitcherpitch))
    except:
        Pitcher_Lead_Swing_P = '---'
    return Pitcher_Lead_Swing_P
def Two_Strikes_Swing_P(df):
    pitcherpitchswing = (df[(df['BS'].isin(["2-2","3-2", "0-2", "1-2"])) & (df['PitchCode'].isin(["Strk-S", "Foul", "In-Play"]))])
    pitcherpitch  = (df[df['BS'].isin(["2-2","3-2", "0-2", "1-2"])])
    try:
        Two_Strikes_Swing_P = '%.1f'%((len(pitcherpitchswing) * 100) / len(pitcherpitch))
    except:
        Two_Strikes_Swing_P = '---'
    return Two_Strikes_Swing_P
def Tie_Swing_P(df):
    tiepitchswing = (df[(df['BS'] .isin(["1-1", "2-2"])) & (df['PitchCode'].isin(["Strk-S", "Foul", "In-Play"]))])
    tiepitch  = (df[df['BS'].isin(["1-1", "2-2"])])
    try:
        Tie_Swing_P = '%.1f'%((len(tiepitchswing) * 100) / len(tiepitch))
    except:
        Tie_Swing_P = '---'
    return Tie_Swing_P

def HeartBall_P(df):
    heart = df[(df['APP_KZoneY'] >= -round(20 / 3, 1)) & (df['APP_KZoneY'] <= round(20 / 3, 1)) & (df['APP_KZoneZ'] <= 38) & (df['APP_KZoneZ'] >= 22)]
    try:
        HeartBall  ='%.1f'%((len(heart) * 100) /len(df))
    except:
        HeartBall = '---'
    return HeartBall
def HeartBall_Swing_P(df):
    heart = df[(df['APP_KZoneY']>=-round(20/3,1))&(df['APP_KZoneY']<=round(20/3,1))&(df['APP_KZoneZ']<=38)&(df['APP_KZoneZ']>=22)]
    heart_s =  df[(df['APP_KZoneY']>=-round(20/3,1)) & (df['APP_KZoneY']<=round(20/3,1)) & (df['APP_KZoneZ']<=38) & (df['APP_KZoneZ'] >= 22) & (df['PitchCode'].isin(["Strk-S","Foul","In-Play"]))]
    try:
        HeartBall_Swing = '%.1f'%((len(heart_s) * 100) / len(heart))
    except:
        HeartBall_Swing = '---'
    return HeartBall_Swing
def wOBA(df):
    wOBA_weights = [0.7775, 0.7846, 0.9578, 1.3347, 1.6917, 2.1009]
    ubb_n = len(df[df['PA_Result'].isin(['BB', 'BB-I', 'BB-IL','BB-P'])])
    hbp_n = len(df[df['PA_Result'].isin(['HBP'])])
    one_n = len(df[df['PA_Result'].isin(['1B'])])
    double_n = len(df[df['PA_Result'].isin(['2B'])])
    triple_n = len(df[df['PA_Result'].isin(['3B'])])
    hr_n = len(df[df['PA_Result'].isin(['HR', 'IHR'])])
    pa = len(df[~(df['PA_Result'].isnull())])
    ab = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR', 'DP', 'E-C', 'E-T', 'F', 'FC', 'FOT', 'G', 'G-', 'GT', 'IF', 'INT', 'K','K-BF','K-DO','K-BS', 'K-DS','Ks','K-SF','LO','K-P'])])
    sf_n = len(df[df['PA_Result'].isin(['SF', 'E-SF'])])
    try:
        wOBA = float('%.3f'%((wOBA_weights[0] * ubb_n + wOBA_weights[1] * hbp_n + wOBA_weights[2] * one_n + wOBA_weights[3] * double_n + wOBA_weights[4] * triple_n + wOBA_weights[5] * hr_n) / (ab + ubb_n + sf_n +hbp_n)))
    except:
        wOBA = '---'
    return wOBA
def First_Pitch_Strike_P(df):
    first_pitch = len(df[df['BS'] == '0-0'])
    first_pitch_strike = len(df[(df['BS'] == '0-0') & (df['PitchCode'] .isin(['Strk-C', 'Strk-S', 'Foul', 'In-Play']))])
    try:
        First_Pitch_Strike = '%.1f'%(first_pitch_strike * 100 / first_pitch)
    except:
        First_Pitch_Strike = '---'
    return First_Pitch_Strike
def Swing_Strike_P(df):
    strk_s = len(df[df['PitchCode'] == 'Strk-S'])
    pitch = len(df[df['PitchCode'].isin(['Strk-S', 'Strk-C', 'Foul', 'In-Play', 'Ball','Ball-B-I'])])
    try:
        swing_strike = '%.1f'%((strk_s * 100) / pitch)
    except:
        swing_strike = '---'
    return swing_strike
def Call_Strike_P(df):
    strk_c = len(df[df['PitchCode'] == 'Strk-C'])
    pitch = len(df[df['PitchCode'].isin(['Strk-S', 'Strk-C', 'Foul', 'In-Play', 'Ball','Ball-B-I'])])
    try:
        call_strike = '%.1f'%((strk_c * 100) / pitch)
    except:
        call_strike = '---'
    return call_strike
def HR_FB_P(df):
    hr = len(df[df['PA_Result'].isin(['HR','IHR'])])
    flyb = len(df[df['HitType'] == 'FLYB'])
    try:
        hr_fb = '%.1f'%(hr * 100 / flyb)
    except:
        hr_fb = '---'
    return hr_fb
def Infield_Hit_P(df):
    infh = len(df[df['HitTag'] == 'INFH'])
    hit = len(df[df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR'])])
    try:
        inf_hit = '%.1f'%((infh * 100) / hit)
    except:
        inf_hit = '---'
    return inf_hit
def Bunt_Hit_P(df):
    bunt_hit_n = len(df[(df['BuntTag'].isin(['SBUNT', 'BUNT'])) & (df['PA_Result'].isin(['1B', '2B', '3B', 'HR', 'IHR']))])
    inplay_bunt = len(df[(df['PitchCode'] == 'In-Play') & (df['BuntTag'].isin(['SBUNT', 'BUNT']))])
    try:
        bunt_hit = '%.1f'%(bunt_hit_n * 100 / inplay_bunt)
    except:
        bunt_hit = '---'
    return bunt_hit
def Bunt_P(df):
    bunt_n = len(df[(df['BuntTag'].isin(['SBUNT', 'BUNT'])) & (df['PitchCode'].isin(['Strk-C', 'Strk-S', 'Foul', 'In-Play']))])
    inplay_bunt = len(df[(df['PitchCode'] == 'In-Play') & (df['BuntTag'].isin(['SBUNT', 'BUNT']))])
    try:
        bunt_p = '%.1f'%(inplay_bunt * 100 / bunt_n)
    except:
        bunt_p = '---'
    return bunt_p
def Pull_P(df):
    pull_n = 0
    inplay = len(df[df['PitchCode'] == 'In-Play'])
    for i in range(0,len(df)):
        Bats = df.at[i, 'BatS']
        if Bats == 0:
            d = df.at[i, 'Theta']
            if 105 <= d <= 140:
                pull_n += 1
                # # try:
                #     pull  = '%.1f'%((pull_n * 100) / inplay)
                # # except:
                #     pull = '---'
        elif Bats == 1:
            d = df.at[i, 'Theta']
            if 40 <= d <= 75:
                pull_n += 1
        try:
            pull  = '%.1f'%((pull_n * 100) / inplay)
        except:
            pull = '---'   
    return pull
def Cent_P(df):
    cent_n = 0
    inplay = len(df[df['PitchCode'] == 'In-Play'])
    for d in df['Theta']:
        if  75 <= d <= 105:
            cent_n += 1
    try:
        cent = '%.1f'%((cent_n * 100) / inplay)
    except:
        cent = '---'
    return cent
def Oppo_P(df):
    oppo_n = 0
    inplay = len(df[df['PitchCode'] == 'In-Play'])
    for i in range(0,len(df)):
        Bats = df.at[0, 'BatS']
        if Bats == 0:
            d = df.at[i, 'Theta']
            if 40 <= d <= 75:
                oppo_n += 1
            # try:
            #     oppo = '%.1f'%((oppo_n * 100) / inplay)
            # except:
            #     oppo = '---'
        elif Bats == 1:
            d = df.at[i, 'Theta']
            if  105 <= d <= 140:
                oppo_n += 1
        try:        
            oppo = '%.1f'%((oppo_n * 100) / inplay) 
        except:
            oppo = '---'
    return oppo

### 20250306 New ###

def Edge_Swing_P(df):
    edge = df[~((df['APP_KZoneY'] >= -round(20/3, 1)) & (df['APP_KZoneY'] <= round(20/3, 1)) & (df['APP_KZoneZ'] <= 38) & (df['APP_KZoneZ'] >= 22))]
    edge_s = df[~((df['APP_KZoneY'] >= -round(20 / 3, 1)) & (df['APP_KZoneY'] <= round(20/3, 1)) & (df['APP_KZoneZ'] <= 38) & (df['APP_KZoneZ'] >= 22)& (df['PitchCode'].isin(["Strk-S","Foul","In-Play"])))]
    try:
        Edge_Swing_P = '%.1f'%((len(edge_s) * 100) / len(edge))
    except:
        Edge_Swing_P = '---'
    return Edge_Swing_P






##########################################################吃排行榜的表#########################################################
def LOB(df):
    hit = int(df.Hit.sum())
    BB = int(df.BB.sum())
    hbp = int(df.HBP.sum())
    R = int(df.R.sum())
    hr = int(df.HR.sum())
    try:
        LOB = '%.1f'%((hit + BB + hbp - R) * 100 / (hit + BB + hbp - (1.4 * hr)))
    except:
        LOB = '---'
    return LOB
def WHIP(df):
    outs = int(df.Outs.sum())
    hit = int(df.Hit.sum())
    BB = int(df.BB.sum())
    try:
        WHIP='%.2f'%(((hit + BB ) * 3 ) / outs)
    except:
        WHIP = '---'
    return WHIP
def BB_9(df):
    BB = int(df.BB.sum())
    outs = int(df.Outs.sum())
    hbp = int(df.HBP.sum())
    try:
        BB_9 = '%.1f'%((BB + hbp)/ outs * 27)
    except:
        BB_9 = '---'
    return BB_9
def K_9(df):
    k = int(df.SO.sum())
    outs = int(df.Outs.sum())
    try:
        SO_9 = '%.1f'%(k / outs * 27)
    except:
        SO_9 = '---'
    return SO_9
def HR_9(df):
    hr = int(df.HR.sum())
    outs = int(df.Outs.sum())
    try:
        HR_9 = '%.1f'%(hr / outs * 27)
    except:
        HR_9 = '---'
    return HR_9
def ERA(df):
    p_Out=int(df.Outs.sum())
    p_ER= int(df.ER.sum())
    try:
        ERA='%.2f'%(p_ER / p_Out * 27)
    except:
        ERA='---'
    return ERA
##########################################################吃Plate_Record的表#########################################################

#df的篩選：player_id跟record_type是P還是BA

def ERA_PlateRecord(df):
    p_ER = df[(df['record_type'] == 'earned') & (df['action_code'] == 'ER')]
    p_Out = len(df[(df['action_code'] == 'IPs') & (df['action_type'] == 'P')])
    try:
        ERA='%.2f'%(len(p_ER) / (p_Out) * 27)
    except:
        ERA='---'
    return ERA
def WHIP_PlateRecord(df):
    outs= len(df[(df['action_code'] == 'IPs') & (df['action_type'] == 'P')])
    hit = len(df[(df['action_code'].isin(['1B', '2B', '3B', 'HR', 'IHR'])) & (df['action_type'] == 'P') & (df['record_type'] == 'earned')])
    BB = len(df[(df['action_code'].isin(['BB','BB-P'])) & (df['action_type'] == 'P') & (df['record_type'] == 'earned')])
    try:
        WHIP='%.2f'%(((hit+BB)*3)/outs)
    except:
        WHIP = '---'
    return WHIP
def BB_9_PlateRecord(df):
    outs= len(df[(df['action_code'] == 'IPs') & (df['action_type'] == 'P')])
    BB = len(df[(df['action_code'].isin(['BB', 'BB-I', 'BB-IL', 'IBB','HBP','BB-P'])) & (df['action_type'] == 'P')])
    try:
        BB_9 = '%.1f'%(BB / outs * 27)
    except:
        BB_9 = '---'
    return BB_9
def K_9_PlateRecord(df):
    k = len(df[(df['action_code'].isin(['K', 'Ks', 'K-DO', 'K-BS', 'K-BF', 'K-DS','K-SF','K-P'])) & (df['action_type'] == 'P')])
    outs= len(df[(df['action_code'] == 'IPs') & (df['action_type'] == 'P')])
    try:
        SO_9 = '%.1f'%(k / outs * 27)
    except:
        SO_9 = '---'
    return SO_9
def HR_9_PlateRecord(df):
    outs= len(df[(df['action_code'] == 'IPs') & (df['action_type'] == 'P')])
    hr = len(df[(df['action_code'].isin(['HR', 'IHR'])) & (df['action_type'] == 'P')])
    try:
        HR_9 = '%.1f'%(hr / outs * 27)
    except:
        HR_9 = '---'
    return HR_9  