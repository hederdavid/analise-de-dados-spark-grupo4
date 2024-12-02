package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count}
import plotly.Bar
import plotly.Plotly.plot
import plotly.element.{Color, Marker}
import plotly.layout.{Axis, Layout}

object Relatorio_7 {
  def show(dfRenomeado: DataFrame): Unit = {
    // Calcular a precipitação média
    val dFprecipMedia = dfRenomeado.agg(avg("precipitacao")).collect()(0)(0).asInstanceOf[Double]

    // Filtrar os dias com precipitação acima da média
    val dfDiasComPrecipitacaoAcimaDaMedia = dfRenomeado.filter(
      col("precipitacao") > dFprecipMedia
    ).select(
      "cidade", "precipitacao"
    )

    dfDiasComPrecipitacaoAcimaDaMedia.show(dfDiasComPrecipitacaoAcimaDaMedia.count().toInt, truncate = false)

    // Contar a frequência de cada cidade
    val dfContagemCidades = dfDiasComPrecipitacaoAcimaDaMedia
      .groupBy("cidade")
      .agg(count("cidade").alias("quantidade"))
      .select("cidade", "quantidade")

    // Exibir os dados das cidades e suas ocorrências


    // Coletar os dados para o gráfico
    val dadosColetados = dfContagemCidades.collect().map(row =>
      (row.getAs[String]("cidade"), row.getAs[Long]("quantidade"))
    )

    val eixoX = dadosColetados.map(_._1) // Nomes das cidades
    val eixoY = dadosColetados.map(_._2) // Quantidade de vezes que aparecem

    // Criar o gráfico de barras
    val grafico = Bar(
      x = eixoX.toSeq,
      y = eixoY.toSeq
    ).withName("Cidades com Precipitação Acima da Média")
      .withMarker(Marker().withColor(Color.RGB(67, 160, 255))) // Cor azul diferenciada

    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Frequência de Cidades com Precipitação Acima da Média")
      .withWidth(800) // Configurar largura para melhor visualização
      .withXaxis(Axis().withTitle("Cidade").withTickangle(-40))
      .withYaxis(Axis().withTitle("Quantidade de Ocorrências"))

    // Caminho onde o gráfico será salvo
    val caminhoArquivo = "relatorios_grafico/7-precipitacao_acima_media.html"

    // Gerar e salvar o gráfico
    plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      openInBrowser = true // Abre automaticamente no navegador
    )
  }
}
/*import org.apache.spark.sql.functions.{col, avg}

object Relatorio_7 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 7 - Dias com precipitação acima da média
    val dFprecipMedia = dfRenomeado.agg(avg("precipitacao").as("Precipitação Média")).collect()(0)(0)

    // Filtrando os dias com precipitação acima da média
    val dfDiasComPrecipitacaoAcimaDaMedia = dfRenomeado.filter(
      col("precipitacao") > dFprecipMedia
    ).select(
      "data", "cidade", "precipitacao"
    )

    // Exibindo os dias com precipitação acima da média
    dfDiasComPrecipitacaoAcimaDaMedia.show(dfDiasComPrecipitacaoAcimaDaMedia.count().toInt, truncate =  false)
  }
}*/


