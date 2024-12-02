package relatorios
import org.apache.spark.sql.DataFrame
import plotly.Bar
import plotly.Plotly.plot
import plotly.element.{Color, Marker}
import plotly.layout.{Axis, Layout}
import org.apache.spark.sql.functions.{col, count}

object Relatorio_3 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 3 - Dias mais chuvosos (> 50 mm)
    val dfDiasMaisChuvosos = dfRenomeado.filter(
      col("precipitacao") > 50
    ).select(
      "data", "cidade", "temperatura_maxima", "temperatura_minima", "precipitacao"
    )

    dfDiasMaisChuvosos.show(dfRenomeado.count().toInt, truncate = false)

    // Contar a frequência de cada cidade nos dias mais chuvosos
    val dfContagemCidadesChuvosas = dfDiasMaisChuvosos
      .groupBy("cidade")
      .agg(count("cidade").alias("quantidade"))
      .select("cidade", "quantidade")

    // Coletar os dados para o gráfico
    val dadosColetados = dfContagemCidadesChuvosas.collect().map(row =>
      (row.getAs[String]("cidade"), row.getAs[Long]("quantidade"))
    )

    val eixoX = dadosColetados.map(_._1) // Nomes das cidades
    val eixoY = dadosColetados.map(_._2) // Quantidade de vezes que aparecem

    // Criar o gráfico de barras
    val grafico = Bar(
      x = eixoX.toSeq,
      y = eixoY.toSeq
    ).withName("Cidades mais Chuvosas (> 50 mm)")
      .withMarker(Marker().withColor(Color.RGB(34, 193, 96))) // Cor diferenciada para o gráfico

    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Cidades com Maior Precipitação (> 50 mm)")
      .withXaxis(Axis().withTitle("Cidade").withTickangle(-40))
      .withYaxis(Axis().withTitle("Quantidade de Ocorrências"))

    // Caminho onde o gráfico será salvo
    val caminhoArquivo = "relatorios_grafico/3-frequencia_cidades_chuvosas.html"

    // Gerar e salvar o gráfico
    plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      openInBrowser = true // Abre automaticamente no navegador
    )

    // Exibir os dados selecionados
    dfContagemCidadesChuvosas.show(false)
  }
}


/*package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Relatorio_3 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 3 - Dias mais chuvosos (> 50 mm)
    val dfDiasMaisChuvosos = dfRenomeado.filter(
      col("precipitacao") > 50
    ).select(
      "data", "cidade", "precipitacao"
    )

    // Exibindo o resultado completo (sem truncamento)
    dfDiasMaisChuvosos.show(false)
  }
}*/
