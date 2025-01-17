
#---------------------------------------------------------------------------------------
# Estimacion de la población total como lo marca el manual de la estimacion de la muestra 
# del censo 2020, pagina 15 (5.8 Estimadores de totales).
#--------------------------------------------------------------------------------------
tab_sexo1<-db %>%
  group_by(MUN,SEXO,FACTOR, ESTRATO,UPM) %>% 
  summarise(N=count()) %>% 
  collect() 

estra<-tab_sexo1 %>% filter(ESTRATO=="01-001-0001-00") %>% arrange(ESTRATO, UPM)

tab_sexo2<-tab_sexo1 %>%
  group_by(MUN, SEXO,ESTRATO) %>% 
  mutate(n=sum(FACTOR*N)) %>% 
  select(MUN, ESTRATO,SEXO,n) %>% 
  unique()

tab_sexo3<-tab_sexo2 %>%
  group_by(MUN,SEXO) %>% 
  mutate(Total=sum(n)) %>% 
  select(MUN,SEXO,Total) %>% 
  unique()




