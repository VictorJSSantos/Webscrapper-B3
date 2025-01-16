SELECT
    acao,
    tipo,
	quantidade,
	data_extracao,
    current_date() AS data_atual,
    datediff(current_date(), data_extracao) AS dias_diferenca
FROM tblRawB3;