select 
    acao, 
    tipo, 
    quantidade, 
    data_extracao, 
    data_atual, 
    dias_diferenca as diferenca_dias 
from "default"."tbl_dados_b3_refinados_glue" 
limit 10;