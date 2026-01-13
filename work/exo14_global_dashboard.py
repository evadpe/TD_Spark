import streamlit as st
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

st.set_page_config(page_title="Weather Analytics Platform", layout="wide", page_icon="🌍")

# Sidebar - Navigation
st.sidebar.title(" Weather Analytics Platform")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    [" Vue d'ensemble", " Données Temps Réel", " Données Historiques", 
     " Records Climatiques", " Profils Saisonniers", "  Anomalies Détectées"]
)

st.sidebar.markdown("---")
st.sidebar.info("""
**Projet:** Système de Monitoring Météo  
**Architecture:** Lambda (Batch + Speed Layer)  
**Technologies:** Kafka, Spark, HDFS, Streamlit
""")

# Fonctions utilitaires
def load_realtime_data(base_path="/home/jovyan/work/hdfs-data"):
    """Charge les alertes temps réel"""
    all_alerts = []
    try:
        for country in os.listdir(base_path):
            country_path = os.path.join(base_path, country)
            if not os.path.isdir(country_path):
                continue
            
            for city in os.listdir(country_path):
                city_path = os.path.join(country_path, city)
                alert_file = os.path.join(city_path, "alerts.json")
                
                if os.path.exists(alert_file):
                    with open(alert_file, 'r') as f:
                        for line in f:
                            if line.strip():
                                try:
                                    all_alerts.append(json.loads(line))
                                except:
                                    pass
        return all_alerts
    except:
        return []

def load_historical_data(city, country):
    """Charge les données historiques d'une ville"""
    file_path = f"/home/jovyan/work/hdfs-data/{country}/{city}/weather_history_raw.json"
    try:
        data = []
        with open(file_path, 'r') as f:
            for line in f:
                if line.strip():
                    data.append(json.loads(line))
        return data
    except:
        return []

def load_records(city, country):
    """Charge les records climatiques"""
    file_path = f"/home/jovyan/work/hdfs-data/{country}/{city}/weather_records.json"
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except:
        return None

def load_seasonal_profile(city, country):
    """Charge le profil saisonnier enrichi"""
    file_path = f"/home/jovyan/work/hdfs-data/{country}/{city}/seasonal_profile_enriched/profile.json"
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except:
        return None

def parse_timestamp(ts):
    """Parse un timestamp"""
    try:
        if 'T' in ts:
            return pd.to_datetime(ts.replace('Z', '+00:00'))
        return pd.to_datetime(ts)
    except:
        return pd.NaT

# ==================== PAGE 1: VUE D'ENSEMBLE ====================
if page == "🏠 Vue d'ensemble":
    st.title("🏠 Vue d'Ensemble du Système")
    
    # Statistiques globales
    alerts = load_realtime_data()
    
    if alerts:
        df = pd.DataFrame(alerts)
        
        # Vérifier que event_time existe
        if 'event_time' in df.columns:
            df['event_time'] = df['event_time'].apply(parse_timestamp)
            df = df.dropna(subset=['event_time'])
        
        if len(df) > 0:
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("📊 Total Alertes", f"{len(df):,}")
            
            with col2:
                st.metric("🌍 Pays", df['country'].nunique())
            
            with col3:
                st.metric("🏙️ Villes", df['city'].nunique())
            
            with col4:
                if 'temperature' in df.columns:
                    avg_temp = df['temperature'].mean()
                    st.metric("🌡️ Temp Moyenne", f"{avg_temp:.1f}°C")
                else:
                    st.metric("🌡️ Temp Moyenne", "N/A")
            
            with col5:
                if 'windspeed' in df.columns:
                    avg_wind = df['windspeed'].mean()
                    st.metric("💨 Vent Moyen", f"{avg_wind:.1f} m/s")
                else:
                    st.metric("💨 Vent Moyen", "N/A")
            
            st.markdown("---")
            
            # Statistiques par ville
            st.subheader("🏙️ Statistiques par Ville")
            
            agg_dict = {'city': 'count'}
            if 'temperature' in df.columns:
                agg_dict['temperature'] = 'mean'
            if 'windspeed' in df.columns:
                agg_dict['windspeed'] = 'mean'
            
            city_stats = df.groupby(['city', 'country']).agg(agg_dict).round(2)
            
            # Renommer les colonnes
            col_names = {'city': 'Nombre Alertes'}
            if 'temperature' in df.columns:
                col_names['temperature'] = 'Temp Moyenne (°C)'
            if 'windspeed' in df.columns:
                col_names['windspeed'] = 'Vent Moyen (m/s)'
            
            city_stats.columns = [col_names.get(col, col) for col in city_stats.columns]
            city_stats = city_stats.sort_values('Nombre Alertes', ascending=False)
            
            st.dataframe(city_stats, use_container_width=True)
            
            # Activité récente
            st.markdown("---")
            st.subheader("📈 Activité Récente")
            
            if 'event_time' in df.columns and 'temperature' in df.columns:
                df_recent = df.sort_values('event_time', ascending=False).head(50)
                
                fig_timeline = px.scatter(
                    df_recent,
                    x='event_time',
                    y='city',
                    color='temperature',
                    size='windspeed' if 'windspeed' in df.columns else None,
                    hover_data=['temperature', 'windspeed'] if 'windspeed' in df.columns else ['temperature'],
                    color_continuous_scale='RdYlBu_r',
                    title='Timeline des Observations (50 dernières)'
                )
                
                st.plotly_chart(fig_timeline, use_container_width=True)
            
            # Distribution des alertes
            if 'wind_alert_level' in df.columns and 'heat_alert_level' in df.columns:
                st.markdown("---")
                st.subheader("⚠️ Distribution des Alertes")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    wind_dist = df['wind_alert_level'].value_counts()
                    fig = px.pie(values=wind_dist.values, names=wind_dist.index, 
                                title='Répartition Alertes Vent')
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    heat_dist = df['heat_alert_level'].value_counts()
                    fig = px.pie(values=heat_dist.values, names=heat_dist.index,
                                title='Répartition Alertes Chaleur')
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Données chargées mais vides après nettoyage.")
    else:
        st.warning("Aucune donnée disponible. Lancez les producteurs et collectez des données.")
        st.info("""
        Pour collecter des données :
        1. Terminal 1: `python exo6_multi_cities_geocoding.py`
        2. Terminal 2: `python exo4_simple_transformation.py`
        3. Terminal 3: `python exo7_local_storage_v2.py`
        
        Attendez quelques minutes puis rafraîchissez cette page.
        """)

# ==================== PAGE 2: DONNÉES TEMPS RÉEL ====================
elif page == " Données Temps Réel":
    st.title(" Monitoring Temps Réel")
    
    if st.button("Rafraîchir"):
        st.rerun()
    
    alerts = load_realtime_data()
    
    if alerts:
        df = pd.DataFrame(alerts)
        df['event_time'] = df['event_time'].apply(parse_timestamp)
        df = df.dropna(subset=['event_time'])
        
        # Filtres
        col1, col2 = st.columns(2)
        with col1:
            countries = ['Tous'] + sorted(df['country'].unique().tolist())
            selected_country = st.selectbox("Pays", countries)
        
        with col2:
            if selected_country != 'Tous':
                cities = ['Toutes'] + sorted(df[df['country'] == selected_country]['city'].unique().tolist())
            else:
                cities = ['Toutes'] + sorted(df['city'].unique().tolist())
            selected_city = st.selectbox("Ville", cities)
        
        # Appliquer filtres
        df_filtered = df.copy()
        if selected_country != 'Tous':
            df_filtered = df_filtered[df_filtered['country'] == selected_country]
        if selected_city != 'Toutes':
            df_filtered = df_filtered[df_filtered['city'] == selected_city]
        
        # Graphiques
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("  Évolution Température")
            fig_temp = px.line(df_filtered, x='event_time', y='temperature', color='city')
            st.plotly_chart(fig_temp, use_container_width=True)
        
        with col2:
            st.subheader("  Évolution Vent")
            fig_wind = px.line(df_filtered, x='event_time', y='windspeed', color='city')
            st.plotly_chart(fig_wind, use_container_width=True)
        
        # Alertes
        st.subheader(" Distribution des Alertes")
        
        col1, col2 = st.columns(2)
        
        with col1:
            wind_alerts = df_filtered['wind_alert_level'].value_counts()
            fig = px.pie(values=wind_alerts.values, names=wind_alerts.index, title='Alertes Vent')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            heat_alerts = df_filtered['heat_alert_level'].value_counts()
            fig = px.pie(values=heat_alerts.values, names=heat_alerts.index, title='Alertes Chaleur')
            st.plotly_chart(fig, use_container_width=True)

# ==================== PAGE 3: DONNÉES HISTORIQUES ====================
elif page == " Données Historiques":
    st.title(" Analyse des Données Historiques (10 ans)")
    
    # Sélection ville
    cities_available = []
    base_path = "/home/jovyan/work/hdfs-data"
    
    try:
        for country in os.listdir(base_path):
            country_path = os.path.join(base_path, country)
            if os.path.isdir(country_path):
                for city in os.listdir(country_path):
                    hist_file = os.path.join(country_path, city, "weather_history_raw.json")
                    if os.path.exists(hist_file):
                        cities_available.append(f"{city}, {country}")
    except:
        pass
    
    if cities_available:
        selected = st.selectbox("Sélectionner une ville", cities_available)
        city, country = selected.split(", ")
        
        hist_data = load_historical_data(city, country)
        
        if hist_data:
            df_hist = pd.DataFrame(hist_data)
            df_hist['date'] = pd.to_datetime(df_hist['date'])
            df_hist['year'] = df_hist['date'].dt.year
            df_hist['month'] = df_hist['date'].dt.month
            
            st.success(f"✓ {len(df_hist):,} jours de données chargés")
            
            # Évolution annuelle
            st.subheader(" Évolution Annuelle")
            
            yearly = df_hist.groupby('year').agg({
                'temp_mean': 'mean',
                'windspeed_max': 'mean',
                'precipitation': 'sum'
            }).reset_index()
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=yearly['year'], y=yearly['temp_mean'], 
                                    name='Température Moyenne', yaxis='y'))
            fig.add_trace(go.Scatter(x=yearly['year'], y=yearly['windspeed_max'], 
                                    name='Vent Moyen', yaxis='y2'))
            
            fig.update_layout(
                title=f'Évolution sur 10 ans - {city}',
                yaxis=dict(title='Température (°C)'),
                yaxis2=dict(title='Vent (m/s)', overlaying='y', side='right')
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Heatmap mensuelle
            st.subheader(" Heatmap Température par Mois/Année")
            
            monthly_temp = df_hist.pivot_table(
                values='temp_mean',
                index='month',
                columns='year',
                aggfunc='mean'
            )
            
            fig_heatmap = px.imshow(
                monthly_temp,
                labels=dict(x="Année", y="Mois", color="Temp (°C)"),
                color_continuous_scale='RdYlBu_r'
            )
            
            st.plotly_chart(fig_heatmap, use_container_width=True)
    else:
        st.warning("Aucune donnée historique disponible. Lancez l'exercice 9.")

# ==================== PAGE 4: RECORDS CLIMATIQUES ====================
elif page == " Records Climatiques":
    st.title(" Records Climatiques")
    
    # Liste des villes avec records
    cities_with_records = []
    base_path = "/home/jovyan/work/hdfs-data"
    
    try:
        for country in os.listdir(base_path):
            country_path = os.path.join(base_path, country)
            if os.path.isdir(country_path):
                for city in os.listdir(country_path):
                    records_file = os.path.join(country_path, city, "weather_records.json")
                    if os.path.exists(records_file):
                        cities_with_records.append(f"{city}, {country}")
    except:
        pass
    
    if cities_with_records:
        selected = st.selectbox("Sélectionner une ville", cities_with_records)
        city, country = selected.split(", ")
        
        records = load_records(city, country)
        
        if records:
            st.subheader(f"Records pour {city}, {country}")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("### Jour le Plus Chaud")
                st.metric("Température", f"{records['hottest_day']['temp_max']}°C")
                st.caption(f" {records['hottest_day']['date']}")
            
            with col2:
                st.markdown("###  Jour le Plus Froid")
                st.metric("Température", f"{records['coldest_day']['temp_min']}°C")
                st.caption(f" {records['coldest_day']['date']}")
            
            with col3:
                st.markdown("###   Rafale la Plus Forte")
                st.metric("Vent", f"{records['windiest_day']['windgusts_max']:.1f} m/s")
                st.caption(f" {records['windiest_day']['date']}")
            
            st.markdown("---")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("###  Période la Plus Pluvieuse")
                st.metric("Précipitations (30j)", f"{records['rainiest_period']['rain_30days']:.1f} mm")
                st.caption(f" Fin de période: {records['rainiest_period']['date']}")
            
            with col2:
                st.markdown("###  Jour le Plus Neigeux")
                st.metric("Neige", f"{records['snowiest_day']['snowfall']:.1f} cm")
                st.caption(f" {records['snowiest_day']['date']}")
            
            # Statistiques globales
            st.markdown("---")
            st.subheader(" Statistiques Globales (10 ans)")
            
            stats = records['statistics']
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Température Max Absolue", f"{stats['absolute_max_temp']}°C")
                st.metric("Température Min Absolue", f"{stats['absolute_min_temp']}°C")
            
            with col2:
                st.metric("Rafale Max Absolue", f"{stats['absolute_max_wind']:.1f} m/s")
                st.metric("Jours Analysés", f"{stats['total_days']:,}")
            
            with col3:
                st.metric("Précipitations Totales", f"{stats['total_precipitation']:.0f} mm")
                st.metric("Chutes de Neige Totales", f"{stats['total_snowfall']:.0f} cm")
    else:
        st.warning("Aucun record disponible. Lancez l'exercice 10.")

# ==================== PAGE 5: PROFILS SAISONNIERS ====================
elif page == " Profils Saisonniers":
    st.title(" Profils Saisonniers")
    
    # Liste des villes avec profils
    cities_with_profiles = []
    base_path = "/home/jovyan/work/hdfs-data"
    
    try:
        for country in os.listdir(base_path):
            country_path = os.path.join(base_path, country)
            if os.path.isdir(country_path):
                for city in os.listdir(country_path):
                    profile_file = os.path.join(country_path, city, "seasonal_profile_enriched/profile.json")
                    if os.path.exists(profile_file):
                        cities_with_profiles.append(f"{city}, {country}")
    except:
        pass
    
    if cities_with_profiles:
        selected = st.selectbox("Sélectionner une ville", cities_with_profiles)
        city, country = selected.split(", ")
        
        profile = load_seasonal_profile(city, country)
        
        if profile:
            st.success(f"✓ Profil saisonnier chargé pour {city}, {country}")
            
            # Créer DataFrame
            profiles_data = []
            for p in profile['profiles']:
                profiles_data.append({
                    'Mois': p['month_name'],
                    'Temp Moyenne': p['temperature']['mean'],
                    'Temp Min': p['temperature']['min'],
                    'Temp Max': p['temperature']['max'],
                    'Temp σ': p['temperature']['stddev'],
                    'Vent Moyen': p['wind']['mean'],
                    'Vent σ': p['wind']['stddev'],
                    'Précipitations': p['precipitation']['mean']
                })
            
            df_profile = pd.DataFrame(profiles_data)
            
            # Graphiques
            st.subheader("  Profil de Température")
            
            fig_temp = go.Figure()
            fig_temp.add_trace(go.Scatter(
                x=df_profile['Mois'],
                y=df_profile['Temp Moyenne'],
                mode='lines+markers',
                name='Moyenne',
                line=dict(width=3)
            ))
            fig_temp.add_trace(go.Scatter(
                x=df_profile['Mois'],
                y=df_profile['Temp Max'],
                mode='lines',
                name='Max',
                line=dict(dash='dash')
            ))
            fig_temp.add_trace(go.Scatter(
                x=df_profile['Mois'],
                y=df_profile['Temp Min'],
                mode='lines',
                name='Min',
                line=dict(dash='dash')
            ))
            
            fig_temp.update_layout(title='Profil de Température', yaxis_title='Température (°C)')
            st.plotly_chart(fig_temp, use_container_width=True)
            
            # Vent
            st.subheader("  Profil de Vent")
            
            fig_wind = px.bar(df_profile, x='Mois', y='Vent Moyen', title='Vent Moyen par Mois')
            st.plotly_chart(fig_wind, use_container_width=True)
            
            # Tableau détaillé
            st.subheader(" Détails Mensuels")
            st.dataframe(df_profile, use_container_width=True)
    else:
        st.warning("Aucun profil saisonnier disponible. Lancez les exercices 11 et 12.")

# ==================== PAGE 6: ANOMALIES ====================
elif page == "  Anomalies Détectées":
    st.title("  Anomalies Climatiques Détectées")
    
    st.info("""
    Cette page afficherait les anomalies en temps réel depuis le topic Kafka `weather_anomalies`.
    
    Pour voir les anomalies en temps réel, utilisez le script dédié:
```bash
    python exo13_anomaly_monitor.py
```
    """)
    
    st.markdown("###  Critères de Détection")
    st.markdown("""
    - **Anomalie de température**: Écart > 2σ par rapport à la moyenne historique du mois
    - **Anomalie de vent**: Écart > 2σ par rapport à la moyenne historique du mois
    - **Types détectés**:
        - Canicule (heat_wave)
        - Vague de froid (cold_wave)
        - Tempête (wind_storm)
    """)

st.sidebar.markdown("---")
st.sidebar.caption(f"Dernière mise à jour: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")