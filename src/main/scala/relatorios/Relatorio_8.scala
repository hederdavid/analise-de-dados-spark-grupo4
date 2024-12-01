package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}
import plotly.Bar
import plotly.Plotly.plot
import plotly.element.{Color, Marker}
import plotly.layout.{Axis, Layout}

object Relatorio_8 {
  def show(dfRenomeado: DataFrame): Unit = {
    // Filtrar os dias de baixa temperatura (< 10°C) e vento forte (> 15 km/h)
    val dFDiasBaixaTemperaturaEVentoForte = dfRenomeado.filter(
      col("temperatura_minima") < 10 &&
        col("velocidade_vento") > 15
    ).select(
      "cidade"
    )

    dFDiasBaixaTemperaturaEVentoForte.show(dFDiasBaixaTemperaturaEVentoForte.count().toInt, truncate = false)

    // Contar a frequência de cada cidade
    val dfContagemCidades = dFDiasBaixaTemperaturaEVentoForte
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
    ).withName("Frequência de Cidades")
      .withMarker(Marker().withColor(Color.RGB(0, 150, 136))) // Cor verde diferenciada

    // Configurar layout do gráfico
    val layout = Layout()
      .withTitle("Frequência de Cidades com Baixa Temperatura e Vento Forte")
      .withWidth(800) // Configurar largura para melhor visualização
      .withXaxis(Axis().withTitle("Cidade").withTickangle(-40))
      .withYaxis(Axis().withTitle("Quantidade de Ocorrências"))

    // Caminho onde o gráfico será salvo
    val caminhoArquivo = "relatorios_grafico/8-baixa_temperatura_vento_forte.html"

    // Gerar e salvar o gráfico
    plot(
      path = caminhoArquivo,
      traces = Seq(grafico),
      layout = layout,
      openInBrowser = true // Abre automaticamente no navegador
    )
  }
}


/*
package relatorios

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Relatorio_8 {
  def show(dfRenomeado: DataFrame): Unit = {
    // 8 - Dias de baixa temperatura e vento forte (mínima < 10°C e vento > 15 km/h)
    val dFDiasBaixaTemperaturaEVentoForte = dfRenomeado.filter(
      col("temperatura_minima") < 10 &&
        col("velocidade_vento") > 15
    ).select(
      "data", "cidade", "temperatura_minima", "velocidade_vento"
    )

    // Exibindo os dias de baixa temperatura e vento forte
    dFDiasBaixaTemperaturaEVentoForte.show(false)
  }
}
*/
