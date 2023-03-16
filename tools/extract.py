import geopandas as gpd
import pandas as pd
from glob import glob
import numpy as np

def fetch_census_tracts(code, files_path):
    code=str(code)
    primeiros_digitos=code[:2]
    file = glob(files_path + '\\Census_Tracts_Geographic_Data\\*\\'+primeiros_digitos+'*.shp')[0]
    gdf = gpd.read_file(file)
    tracts_gdf = gdf[gdf['CD_GEOCODM']==code].copy()
    
    return tracts_gdf

def fetch_demographic_data(code,files_path,income=False,race=False,
                           literacy=False,sex=False,age_groups=False):
    #inicia obtendo os setores censitários:
    tracts = fetch_census_tracts(code, files_path)
    
    ##início do código aqui##
    
    
    #fusão com dados básicos(domicilios e pessoas)
    csv_basico_location = files_path+'\\Cesus_Tracts_Statistic_Data\\Basico.csv'
    stat_data_basico = pd.read_csv(csv_basico_location, sep=';')
    stat_data_basico = stat_data_basico[['Cod_setor','V001','V002']]
    stat_data_basico.rename(columns={'Cod_setor':'CD_GEOCODI',
                          'V001':'Domicilios',
                          'V002':'Pessoas'},
                 inplace=True)
    tracts['CD_GEOCODI'] = np.int64(tracts['CD_GEOCODI'])
    tracts = tracts.merge(stat_data_basico, on='CD_GEOCODI')
    
    if income:
        csv_income_location = files_path+'\\Cesus_Tracts_Statistic_Data\\PessoaRenda.csv'
        stat_data_income = pd.read_csv(csv_income_location, sep=';')
        stat_data_income = stat_data_income[['Cod_setor','V001' , 'V002', 'V003', 'V004', 'V005', 'V006', 'V007', 'V008' , 'V009' , 'V010']]
        stat_data_income.rename(columns={'Cod_setor':'CD_GEOCODI',
                          'V001':'Renda_ate_1/2_salario_minimo',
                          'V002':'Renda_de_1/2_a_1_salario_minimo',
                          'V003':'Renda_de_1_a_2_salarios_minimos',
                          'V004':'Renda_de_2_a_3_salarios_minimos',
                          'V005':'Renda_de_3_a_5_salarios_minimos',
                          'V006':'Renda_de_5_a_10_salarios_minimos',
                          'V007':'Renda_de_10_a_15_salarios_minimos',
                          'V008':'Renda_de_15_a_20_salarios_minimos',
                          'V009':'Renda_maior_20_salarios_minimos',
                          'V010':'Sem_renda'},
                 inplace=True)
        tracts = tracts.merge(stat_data_income, on='CD_GEOCODI')
        
    if race:
        csv_race_location = files_path+'\\Cesus_Tracts_Statistic_Data\\Pessoa03.csv'
        stat_data_race = pd.read_csv(csv_race_location, sep=';')
        stat_data_race = stat_data_race[['Cod_setor','V002' , 'V003', 'V004', 'V005', 'V006']]
        stat_data_race.rename(columns={'Cod_setor':'CD_GEOCODI',
                          'V002':'Branca',
                          'V003':'Preta',
                          'V004':'Amarela',
                          'V005':'Parda',
                          'V006':'Índigina'},
                inplace=True)
        tracts = tracts.merge(stat_data_race, on='CD_GEOCODI')

    if literacy:
        csv_literacy_location = files_path+'\\Cesus_Tracts_Statistic_Data\\Pessoa01.csv'
        stat_data_literacy = pd.read_csv(csv_literacy_location, sep=';')
        stat_data_literacy = stat_data_literacy[['Cod_setor','V001']]
        stat_data_literacy.rename(columns={'Cod_setor':'CD_GEOCODI',
                          'V001':'Alfabetizados'},
                inplace=True)
        tracts = tracts.merge(stat_data_literacy, on='CD_GEOCODI')
    
    if sex:
        csv_sex_man_location = files_path+'\\Cesus_Tracts_Statistic_Data\\Pessoa11.csv'
        csv_sex_woman_location = files_path+'\\Cesus_Tracts_Statistic_Data\\Pessoa12.csv'
        stat_data_sex_man = pd.read_csv(csv_sex_man_location, sep=';')
        stat_data_sex_man = stat_data_sex_man[['Cod_setor','V001']]
        stat_data_sex_man.rename(columns={'Cod_setor':'CD_GEOCODI',
                          'V001':'Homens'},
                inplace=True)
        tracts = tracts.merge(stat_data_sex_man, on='CD_GEOCODI')
        stat_data_sex_woman = pd.read_csv(csv_sex_woman_location, sep=';')
        stat_data_sex_woman = stat_data_sex_woman[['Cod_setor','V001']]
        stat_data_sex_woman.rename(columns={'Cod_setor':'CD_GEOCODI',
                          'V001':'Mulheres'},
                inplace=True)
        tracts = tracts.merge(stat_data_sex_woman, on='CD_GEOCODI')
    
    if age_groups:
       csv_age_location = files_path+'\\Cesus_Tracts_Statistic_Data\\Pessoa13.csv'
       stat_data_age = pd.read_csv(csv_age_location, sep=';')
       stat_data_age = stat_data_age[['Cod_setor','V022','V035','V036','V037','V038','V039','V040','V041','V042','V043','V044','V045','V046','V047','V048','V049','V050','V051','V052','V053','V054','V055','V056','V057','V058','V059','V060','V061','V062','V063','V064','V065','V066','V067','V068','V069','V070','V071','V072','V073','V074','V075','V076','V077','V078','V079','V080','V081','V082','V083','V084','V085','V086','V087','V088','V089','V090','V091','V092','V093','V094','V095','V096','V097','V098','V099','V100','V101','V102','V103','V104','V105','V106','V107','V108','V109','V110','V111','V112','V113','V114','V115','V116','V117','V118','V119','V120','V121','V122','V123','V124','V125','V126','V127','V128','V129','V130','V131','V132','V133','V134']]
       stat_data_age.rename(columns={'Cod_setor':'CD_GEOCODI','V022':'<1ano','V035':'1ano','V036':'2anos','V037':'3anos','V038':'4anos','V039':'5anos','V040':'6anos','V041':'7anos','V042':'8anos','V043':'9anos','V044':'10anos','V045':'11anos','V046':'12anos','V047':'13anos','V048':'14anos','V049':'15anos','V050':'16anos','V051':'17anos','V052':'18anos','V053':'19anos','V054':'20anos','V055':'21anos','V056':'22anos','V057':'23anos','V058':'24anos','V059':'25anos','V060':'26anos','V061':'27anos','V062':'28anos','V063':'29anos','V064':'30anos','V065':'31anos','V066':'32anos','V067':'33anos','V068':'34anos','V069':'35anos','V070':'36anos','V071':'37anos','V072':'38anos','V073':'39anos','V074':'40anos','V075':'41anos','V076':'42anos','V077':'43anos','V078':'44anos','V079':'45anos','V080':'46anos','V081':'47anos','V082':'48anos','V083':'49anos','V084':'50anos','V085':'51anos','V086':'52anos','V087':'53anos','V088':'54anos','V089':'55anos','V090':'56anos','V091':'57anos','V092':'58anos','V093':'59anos','V094':'60anos','V095':'61anos','V096':'62anos','V097':'63anos','V098':'64anos','V099':'65anos','V100':'66anos','V101':'67anos','V102':'68anos','V103':'69anos','V104':'70anos','V105':'71anos','V106':'72anos','V107':'73anos','V108':'74anos','V109':'75anos','V110':'76anos','V111':'77anos','V112':'78anos','V113':'79anos','V114':'80anos','V115':'81anos','V116':'82anos','V117':'83anos','V118':'84anos','V119':'85anos','V120':'86anos','V121':'87anos','V122':'88anos','V123':'89anos','V124':'90anos','V125':'91anos','V126':'92anos','V127':'93anos','V128':'94anos','V129':'95anos','V130':'96anos','V131':'97anos','V132':'98anos','V133':'99anos','V134':'100anos+'},inplace=True)
       stat_data_age['age_0-11']=stat_data_age['<1ano']+stat_data_age['1ano']+stat_data_age['2anos']+stat_data_age['3anos']+stat_data_age['4anos']+stat_data_age['5anos']+stat_data_age['6anos']+stat_data_age['7anos']+stat_data_age['8anos']+stat_data_age['9anos']+stat_data_age['10anos']
       stat_data_age['age_11-18']=stat_data_age['11anos']+stat_data_age['12anos']+stat_data_age['13anos']+stat_data_age['14anos']+stat_data_age['15anos']+stat_data_age['16anos']+stat_data_age['17anos']
       stat_data_age['age_18-25']=stat_data_age['18anos']+stat_data_age['19anos']+stat_data_age['20anos']+stat_data_age['21anos']+stat_data_age['22anos']+stat_data_age['23anos']+stat_data_age['24anos']                                     
       stat_data_age['age_25-35']=stat_data_age['25anos']+stat_data_age['26anos']+stat_data_age['27anos']+stat_data_age['28anos']+stat_data_age['29anos']+stat_data_age['30anos']+stat_data_age['31anos']+stat_data_age['32anos']+stat_data_age['33anos']+stat_data_age['34anos']
       stat_data_age['age_35-60']=stat_data_age['35anos']+stat_data_age['36anos']+stat_data_age['37anos']+stat_data_age['38anos']+stat_data_age['39anos']+stat_data_age['40anos']+stat_data_age['41anos']+stat_data_age['42anos']+stat_data_age['43anos']+stat_data_age['44anos']+stat_data_age['45anos']+stat_data_age['46anos']+stat_data_age['47anos']+stat_data_age['48anos']+stat_data_age['49anos']+stat_data_age['50anos']+stat_data_age['51anos']+stat_data_age['52anos']+stat_data_age['53anos']+stat_data_age['54anos']+stat_data_age['55anos']+stat_data_age['56anos']+stat_data_age['57anos']+stat_data_age['58anos']+stat_data_age['59anos']                                     
       stat_data_age['age_60-75']=stat_data_age['60anos']+stat_data_age['61anos']+stat_data_age['62anos']+stat_data_age['63anos']+stat_data_age['64anos']+stat_data_age['65anos']+stat_data_age['66anos']+stat_data_age['67anos']+stat_data_age['68anos']+stat_data_age['69anos']+stat_data_age['70anos']+stat_data_age['71anos']+stat_data_age['72anos']+stat_data_age['73anos']+stat_data_age['74anos']
       stat_data_age['age_75+']=stat_data_age['75anos']+stat_data_age['76anos']+stat_data_age['77anos']+stat_data_age['78anos']+stat_data_age['79anos']+stat_data_age['80anos']+stat_data_age['81anos']+stat_data_age['82anos']+stat_data_age['83anos']+stat_data_age['84anos']+stat_data_age['85anos']+stat_data_age['86anos']+stat_data_age['87anos']+stat_data_age['88anos']+stat_data_age['89anos']+stat_data_age['90anos']+stat_data_age['91anos']+stat_data_age['92anos']+stat_data_age['93anos']+stat_data_age['94anos']+stat_data_age['95anos']+stat_data_age['96anos']+stat_data_age['97anos']+stat_data_age['98anos']+stat_data_age['99anos']+stat_data_age['100anos+']
       stat_data_age = stat_data_age[['CD_GEOCODI','age_0-10','age_11-18','age_18-25','age_25-35','age_35-60','age_60-75','age_75+']]
       tracts = tracts.merge(stat_data_age, on='CD_GEOCODI')
       
    return tracts