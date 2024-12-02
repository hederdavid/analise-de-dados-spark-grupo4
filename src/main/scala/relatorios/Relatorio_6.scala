package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}
import plotly.Bar
import plotly.Plotly.plot
import plotly.element.{Color, Marker}
import plotly.layout.{Axis, Layout}

object Relatorio_6 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 6 - Dias com vento mais intenso (> 20 km/h)
    val dFDiasComVentoIntenso = dfRenomeado.filter(
      col("velocidade_vento") > 20
    ).select(
      "cidade", "velocidade_vento"
    )

    dFDiasComVentoIntenso.show(dFDiasComVentoIntenso.count().toInt, truncate = false)

    // Contar a frequência de cada cidade
    val dfContagemCidades = dFDiasComVentoIntenso
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
    ).withName("Cidades com Vento Intenso (> 20 km/h)")
      .withMarker(Marker().withColor(Color.RGB(34, 193, 96))) // Cor verde agradável

    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Frequência de Cidades com Vento Intenso (> 20 km/h)")
      .withXaxis(Axis().withTitle("Cidade").withTickangle(-40))
      .withYaxis(Axis().withTitle("Quantidade de Ocorrências"))

    // Caminho onde o gráfico será salvo
    val caminhoArquivo = "relatorios_grafico/6-vento_intenso_ocorrencias.html"

    // Gerar e salvar o gráfico
    plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      openInBrowser = true // Abre automaticamente no navegador
    )
  }
}



/*import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Relatorio_6 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 6 - Dias com vento mais intenso (> 20 km/h)
    val dFDiasComVentoIntenso = dfRenomeado.filter(
      col("velocidade_vento") > 20
    ).select(
      "data", "cidade", "velocidade_vento"
    )

    // Exibindo os dias com vento mais intenso
    dFDiasComVentoIntenso.show(false)
  }
}*/
