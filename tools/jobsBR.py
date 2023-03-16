import geopandas as gpd
import random
import pandas as pd
import pickle
import numpy as np

def get_jobs_in_region(gdf,uf_code,rais_df,path='',
                       convex_hull=False, separate=False):
    if convex_hull:
        poly = gdf.unary_union.convex_hull.buffer(.001)
    else:
        poly = gdf.unary_union
    
    geom = gpd.GeoDataFrame({'ID':[0]},geometry=[poly],crs=gdf.crs)
    
    logr = pickle.load(open(path+f'\\gdfs\\_{uf_code}.gdf','rb'))
    logr = logr.to_crs(gdf.crs)
    
    logr['geometry'] = logr.representative_point()
    logr['Comercial'] = logr['TOT_GERAL']-logr['TOT_RES']
    
    jobs = gpd.sjoin(logr,geom).drop(['index_right','ID'],axis=1)
    try:
        jobs['CEP'] = np.int64(jobs['CEP'])
    except:
        jceps=[]
        for jcep in jobs['CEP']:
            try:
                jceps.append(int(jcep))
            except:
                jceps.append(-1)
        jobs['CEP'] = np.int64(jceps)
    jobs = jobs[jobs['Comercial']>0]
    
    temp = jobs['CEP'].unique()
    rais_df = rais_df[(rais_df['CEP Estab']<99999999) & (rais_df['CEP Estab']>1000000)].copy()
    rais_df = rais_df[rais_df['CEP Estab'].isin(temp)]
    rais_df = rais_df[rais_df['Qtd Vínculos Ativos']>0]
    if separate:
        filt_dict = {
            1:[n for n in range(1,16)],  #industria
            2:[n for n in range(16,20)], #comércio
            3:[20,21], #serviços
            4:[22], #saúde
            5:[23], #ensino
            6:[24], #adm pública
            7:[25]  #agricultura
        }
        temps = {n:rais_df[rais_df['IBGE Subsetor'].isin(filt_dict[n])] for n in filt_dict}
        results = {}
        for n,temp in temps.items():
            results[n] = {}
            for cep in jobs['CEP'].unique():
                results[n] = distribute_jobs(jobs,temp,cep,results[n])
    else:
        results = {0:{}}
        for cep in jobs['CEP'].unique():
            results[0] = distribute_jobs(jobs,rais_df,cep,results[0])
    
    for n,res in results.items():
        col = []
        for code in jobs['cod_face']:
            if code in res:
                col.append(res[code])
            else:
                print(f'{code} not found')
                col.append(0)
        jobs[f'C{n}_jobs'] = col
    if 'C0_jobs' not in jobs.columns:
        jobs[f'C0_jobs'] = 0
        for n in results:
            jobs['C0_jobs'] = jobs['C0_jobs']+jobs[f'C{n}_jobs']
    jobs['jobs'] = jobs['C0_jobs']
    return jobs[jobs['jobs']>0]

def distribute_jobs(logr,rais_df,cep, add_to,
                    column_rais='Qtd Vínculos Ativos',
                    column_logr='Comercial'):
    temp_logr = logr[logr['CEP']==cep]
    tot_jobs = rais_df[rais_df['CEP Estab']==cep][column_rais].sum()
    tot_coms = logr[column_logr].sum()
    
    for code, coms in zip(logr['cod_face'],logr[column_logr]):
        if code in add_to:
            add_to[code] += tot_jobs/tot_coms*coms
        else:
            add_to[code] = tot_jobs/tot_coms*coms
    return add_to
	
def get_population(gdf,G,side=250):
    gdf = gdf.copy()
    gdf['geometry'] = gdf.buffer(0).geometry
    region = gpd.GeoDataFrame(geometry=[gdf.unary_union.convex_hull],
                              crs=gdf.crs)
    hex_grid = get_hex_grid(region,side)
    hex_grid = remove_excess_polys(G,hex_grid,drop_duplicates=True,buffer=0)
    hex_grid['HEX_ID'] = [n for n in range(len(hex_grid))]
    
    join = gpd.sjoin(gdf,hex_grid)
    base_crs = hex_grid.crs
    join = ox.project_gdf(join)
    hex_grid = hex_grid.to_crs(join.crs)
    
    pops = []
    for i,row in hex_grid.iterrows():
        idx = row['HEX_ID']
        base_geom = row['geometry']
        temp = join[join['HEX_ID']==idx]
        p = 0
        for geom,population in zip(temp.geometry,temp['POP']):
            p+=geom.intersection(base_geom).area/geom.area*population
        pops.append(p)
    hex_grid = hex_grid.to_crs(base_crs)
    hex_grid['population'] = pops
    
    return hex_grid
	
def get_stat_grid(gdf,path):
    index = gpd.read_file(path+'index.shp')
    files = list(gpd.sjoin(gdf,index.to_crs(gdf.crs))['file'].unique())
    files = [path+f for f in files]
    grid = None
    for file in files:
        temp = gpd.read_file(file)
        temp = gpd.sjoin(temp,gdf.to_crs(temp.crs)).drop_duplicates(subset=['ID_UNICO'])[temp.columns]
        if grid is None:
            grid=temp.copy()
        else:
            grid = grid.append(temp)
    return grid
	
def fair_redistr(jobs,column='jobs',seed=None):
    random.seed(seed)
    dec = jobs[column].sum()-np.int64(jobs[column]).sum()
    dec = int(dec)
    
    j = [n for n in np.int64(jobs[column])]
    non_zero_indices=list(np.where(np.array(j)>0)[0])
    if len(non_zero_indices)==0:
        jobs[column] = np.nan
        print('no place has more than 1 job')
        return None
        raise ValueError('no place has more than 1 job')
    while dec >= len(non_zero_indices):
        for n in non_zero_indices:
            j[n]+=1
        dec = dec-len(non_zero_indices)
        
    if dec>0:
        add_to = random.sample(non_zero_indices,k=dec)
    for i in add_to:
        j[i]+=1
    jobs[column]=j
    return None