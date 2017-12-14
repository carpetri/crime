library(tidyverse)
library(lubridate)
library(foreign)
library(Hmisc)
library(rgdal)
library(ggplot2)
library(stringr)

theme_map <- theme(axis.line=element_blank(),
                    axis.text.x=element_blank(),
                    axis.text.y=element_blank(),
                    axis.ticks=element_blank(),
                    axis.title.x=element_blank(),
                    axis.title.y=element_blank(),
                    panel.background=element_blank(),
                    panel.border=element_blank(),
                    panel.grid.major=element_blank(),
                    panel.grid.minor=element_blank(),
                    plot.background=element_blank(),
                    legend.position="bottom")

crimes <- read_csv('../data/crime_clean.csv') %>% filter(!is.na(taxi_zone_id)) %>% 
  mutate(year = year(stating_datetime) ) 

taxi_zones <- read.dbf('../data/taxi_zones/taxi_zones_clean.dbf') %>% 
  select(zone,LocationID,borough) %>% rename(taxi_zone_id = LocationID) %>% 
  as_data_frame()

dat <- crimes %>%
  group_by(year) %>% 
  filter(year >2008) %>% filter( year < 2017) %>% 
  dplyr::summarize( n_crimes = n()) %>% 
  arrange( desc(n_crimes) ) 
  
dat %>% ggplot( aes(x= year, y=n_crimes) )  +
    geom_line() + theme_classic() +
  labs(x='Year',y ='Number crimes')
ggsave('../img/total_crimes.pdf',height = 2, width = 6)

zones_shp <- readOGR('../data/taxi_zones/',layer = 'taxi_zones_clean')
zones_shp@data  <- zones_shp@data  %>% dplyr::rename(taxi_zone_id = LocationID)



l_ply(2008:2016,function(y){
  dat <- crimes %>%
    group_by(year , taxi_zone_id) %>% 
    filter(year == y) %>% 
    dplyr::summarize( n_crimes = n()) %>% 
    left_join(taxi_zones) %>% 
    arrange( desc(n_crimes) ) %>%
    mutate( n_crimes_group = cut2(n_crimes,g = 5 ))
  
  dat$taxi_zone_id %>% unique %>% as.numeric %>% sort
  

  
  dat$taxi_zone_id <- as.character(dat$taxi_zone_id)
  zones_f <- fortify(zones_shp,region = 'taxi_zone_id') %>% 
    dplyr::rename(taxi_zone_id= id ) %>% left_join(dat)
  
  zones_f %>% head
  zones_f[ is.na(zones_f$n_crimes_group),'n_crimes_group'] <- levels(zones_f$n_crimes_group)[1]
  
  levels(zones_f$n_crimes_group) <- paste0('Quantile ',1:5)
  ggplot(zones_f , aes(x=long, y = lat, group=group , order=order)) +
      theme_map +
      geom_polygon( aes(fill = n_crimes_group)) +
      geom_path() + 
      labs(fill='Number of crimes') + scale_fill_brewer(palette="RdYlGn",direction = -1)
  ggsave(paste0('../img/crimes_per_zone_',y,'.pdf'),height = 6, width = 6)
}
)

boros <- str_to_title( crimes$borough %>% unique %>% as.character() )

for ( y in 2008:2015){
for(b in boros[1:5]){
  print(b)
  print(y)
    dat <- crimes %>%
    group_by(year , taxi_zone_id) %>% 
    dplyr::filter(year == y ) %>% 
    dplyr::summarize( n_crimes = n()) %>% 
    left_join(taxi_zones) %>%
    dplyr::filter(year == y & borough == b) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate( n_crimes_group = cut2(n_crimes,g = 5 ))

  dat$taxi_zone_id <- as.character(dat$taxi_zone_id)
  zones_f <- fortify(zones_shp,region = 'taxi_zone_id') %>% 
    dplyr::rename(taxi_zone_id= id ) %>% left_join(dat) %>% 
    filter(borough == b)
  
  zones_f[ is.na(zones_f$n_crimes_group),'n_crimes_group'] <- levels(zones_f$n_crimes_group)[1]
  # levels(zones_f$n_crimes_group) <- paste0('Quantile ',1:5)
  ggplot(zones_f , aes(x=long, y = lat, group=group , order=order)) +
    theme_map +
    geom_polygon( aes(fill = n_crimes_group)) +
    geom_path() +   
    #coord_map( orientation = c(90-21, 0,0) ) +
    theme(legend.position="right") +
    labs(fill='Crime level') + scale_fill_brewer(palette="RdYlGn",direction = -1)
  ggsave(paste0('../img/crimes_per_zone_',y,'_',b,'.pdf'),height = 5, width = 5)
  }}



# dat <- crimes %>%
#   group_by(year,offense_level) %>% 
#   filter(year >2008) %>% 
#   filter( year < 2017) %>% 
#   dplyr::summarize( n_crimes = n()) %>% 
#   arrange( desc(n_crimes) ) 
# 
# dat %>% ggplot( aes(x= year, y=n_crimes, group = offense_level, color= offense_level) )  +
#   geom_line() + theme_classic() +
#   labs(x='Year',y ='Number crimes')
# 
# crimes
# dat <- crimes %>%
#   dplyr::select(offense_level,ofense_description)  %>% unique %>% arrange(offense_level)
# 
# dat %>% write_csv('../data/offense_level_vs_description.csv')



crimes %>% head


taxi_yz <- read_csv('../data/taxis_per_year_per_zone.csv') %>%
  dplyr::select(-X1) %>% dplyr::rename(taxi_zone_id =  pickup_location_id, n_pick = count) 
  

dat_crimes <-  crimes %>%
  group_by(year , taxi_zone_id) %>% 
  filter(year == y) %>% 
  dplyr::summarize( n_crimes = n()) %>% 
  left_join(taxi_zones) %>% 
  arrange( desc(n_crimes) ) 
dat_crimes



for ( y in 2008:2015){
  # for(b in boros[1:5]){
    print(b)
    print(y)
  dat <- taxi_yz %>%
    filter(year == y) %>% 
    left_join(taxi_zones) %>% 
    # filter(borough==b) %>% 
    left_join(dat_crimes) %>% 
    dplyr::mutate( count_b = n_crimes  /n_pick ) %>% 
    group_by(year ,  taxi_zone_id) %>% 
    dplyr::mutate( count_b = n_pick / (100*n_crimes) ) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(count_group = cut2(round(count_b,digits = 2),g = 5 ) )

  dat$taxi_zone_id <- as.character(dat$taxi_zone_id)
  
  zones_f <- fortify(zones_shp,region = 'taxi_zone_id') %>% 
    dplyr::rename(taxi_zone_id= id ) %>% left_join(dat) %>% 
    filter(borough %in% (dat$borough %>% unique) )
  
  zones_f[ is.na(zones_f$count_b),'count_group'] <- levels(zones_f$count_group)[1]
  # levels(zones_f$count_group) <- paste0('Quantile ',1:5)
  ggplot(zones_f , aes(x=long, y = lat, group=group , order=order)) +
    theme_map +
    geom_polygon( aes(fill = count_group)) +
    geom_path() + 
    labs(fill='Pickups / crime') + scale_fill_brewer(palette="RdYlGn",direction = 1)
  # ggsave(paste0('../img/taxis_per_crime_',y,'_',b,'.pdf'),height = 5, width = 3)
})

taxi_yz

for ( y in 2009:2015){
  for(b in boros[1:5]){
  print(b)
  print(y)
  dat <- taxi_yz %>%
    filter(year == y) %>% 
    left_join(taxi_zones) %>% 
    filter(borough==b) %>%
    left_join(dat_crimes) %>%
    group_by(year ,  taxi_zone_id) %>% 
    dplyr::mutate( count_b = n_pick ) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(count_group = cut2(round(count_b,digits = 2),g = 5 ) )
  
  dat$taxi_zone_id <- as.character(dat$taxi_zone_id)
  
  zones_f <- fortify(zones_shp,region = 'taxi_zone_id') %>% 
    dplyr::rename(taxi_zone_id= id ) %>% left_join(dat) %>% 
    filter(borough %in% (dat$borough %>% unique) )
  
  zones_f[ is.na(zones_f$count_b),'count_group'] <- levels(zones_f$count_group)[1]
  # levels(zones_f$count_group) <- paste0('Quantile ',1:5)
  ggplot(zones_f , aes(x=long, y = lat, group=group , order=order)) +
    theme_map +
    geom_polygon( aes(fill = count_group)) +
    geom_path() + 
    theme(legend.position="right") +
    labs(fill='Pickups') + scale_fill_brewer(palette="RdYlGn",direction = 1)
    ggsave(paste0('../img/taxis_',y,'_',b,'.pdf'),height = 5, width = 5 )
}}




taxi_yz <- read_csv('../data/taxis_per_year_per_zone.csv') %>%
  dplyr::select(-X1) %>% dplyr::rename(taxi_zone_id =  pickup_location_id, n_pick = count) 


dat_crimes <-  crimes %>%
  group_by(year , taxi_zone_id) %>% 
  dplyr::summarize( n_crimes = n()) %>% 
  left_join(taxi_zones) %>% 
  arrange( desc(n_crimes) )  
dat_crimes

dat <- taxi_yz %>%
  left_join(taxi_zones) %>% 
  left_join(dat_crimes) %>%
  dplyr::filter(!'Airport' %in% zone) %>% 
  group_by(year ,  taxi_zone_id) %>% 
  dplyr::mutate( count_b = n_pick ) %>% 
  dplyr::ungroup() %>% 
  dplyr::filter(borough!='EWR') %>% 
  dplyr::mutate(borogh = as.character(borough)) 

dat[!grepl( 'Airport',(dat$zone %>% as.character() ) ),] %>% 
  ggplot(aes(x=n_pick, y = n_crimes))+ 
  geom_point()+ 
  scale_x_log10()+
  theme_bw() +
  facet_wrap(~borough,scales = 'free',ncol = 2)+
  geom_smooth(color='red',method = "lm", formula = y ~ splines::bs(x, 1)) +
  # geom_smooth(color='red',method = "lm", formula = y ~ splines::bs(x, 1)) +
  labs(x='Log(number of pickups)', y = 'Total crimes', caption='*Excluding airports trips')
ggsave(paste0('../img/scatter_crimes_taxis.pdf'),height = 7, width = 6 )

dat[ 'airport' %in% dat$zone ,]

# dat[dat$taxi_zone_id != 138 & dat$taxi_zone_id != 132 &  , ]



taxi_yz_2 <- read_csv('../data/taxi_data_hourly_weather_crime.csv')



taxis_yzw <- taxi_yz_2 %>% 
   dplyr::rename(taxi_zone_id =  pickup_location_id) 

dat <- taxis_yzw %>%
  left_join(taxi_zones) %>% 
  left_join(dat_crimes) %>%
  dplyr::filter(!grepl( 'Airport',(zone %>% as.character() ) )) %>% 
  dplyr::filter(borough!='EWR') %>% 
  dplyr::mutate(borogh = as.character(borough)) %>% 
  dplyr::mutate(rain = rain_hour > 10 ) %>% 
    group_by(year ,  taxi_zone_id, zone, borough, rain) %>% 
    dplyr::summarise(n_pick = sum(n_pickups_hour), n_crimes=sum(n_crimes)) %>% 
    dplyr::ungroup() 

dat_crimes


#Is the average number of taxi pickups different in areas that 
# have different levels of crime rates when we compare at times 
# with and without rain (or different weather variables)?

dat
dat %>% dplyr::filter(!is.na(rain)) %>% 
  ggplot(aes(x=n_pick, y = n_crimes, group = rain , colour = rain))+ 
  geom_point()+ 
  scale_x_log10()+
  theme_bw() + 
  facet_grid(rain~borough,scales = 'free',ncol = 2)+
  geom_smooth(color='red',method = "lm", formula = y ~ splines::bs(x, 1)) +
  # geom_smooth(color='red',method = "lm", formula = y ~ splines::bs(x, 1)) +
  labs(x='Log(number of pickups)', y = 'Total crimes', caption='*Excluding airports trips')

ggsave(paste0('../img/scatter_crimes_taxis.pdf'),height = 7, width = 6 )


