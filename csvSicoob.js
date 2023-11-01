const mysql = require('mysql');
const csvToJson = require('convert-csv-to-json');
const fs = require('fs');
const dir = './csv';
const dirJson = './json';
const dirJsonFormatado = './formatado';
const dirSQL = './sql';
const user = 171;
const data = "2023-03-15"

const connection = mysql.createConnection({
  host: "",
  user: "",
  password: "",
  database: ''
});

const contratantes = [
  { id: 5, contrato: '2554' },
  { id: 6, contrato: '2555' },
  { id: 7, contrato: '5952' },
  { id: 8, contrato: '0593' }
]

function formatarCPF(cpf) {
  return cpf.replace(/\.|-/g, '');
}

function getIdContratante(fileName) {
  const result = contratantes.find(item => fileName.includes(item.contrato))
  return result.id
}

function buscarIdTitularEIdPorCPF(cpf) {
  try {
    // Carregue o conteúdo do arquivo JSON
    const jsonData = JSON.parse(fs.readFileSync('./data.json', 'utf8'));

    // Procure o objeto com o CPF fornecido
    const resultado = jsonData.find(item => item.cpf === formatarCPF(cpf));

    if (resultado) {
      return {
        idtitular: resultado.idtitular,
        iddependente: resultado.iddependente,
        conta: resultado.conta
      };
    } else {
      return null;
    }
  } catch (error) {
    console.error('Erro ao ler o arquivo JSON:', error);
    return null;
  }
}

function executeSQLQuery(query) {
  return new Promise((resolve, reject) => {
    connection.query(query, (error, results) => {
      if (error) {
        reject(error);
      } else {
        resolve(results);
      }
    });
  });
}

async function main() {
  try {
    await new Promise((resolve, reject) => {
      connection.connect((err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });

    //Query
    const sqlQuery = 'SELECT unimeduberlandia_dependente.id as iddependente, idtitular,conta,unimeduberlandia_dependente.cpf,cpftitular FROM unimeduberlandia_dependente INNER JOIN unimeduberlandia_titular ON unimeduberlandia_dependente.idtitular = unimeduberlandia_titular.id;';
    const results = await executeSQLQuery(sqlQuery);

    //Salva resultado dependentes em um data.json
    fs.writeFileSync('./data.json', JSON.stringify(results), 'utf-8')

    //Lista arquivos no direito ./csv
    const fileList = fs.readdirSync(dir)

    fileList.forEach(file => {
      //Faz a leitura do arquivo .csv de mensalidade para um json
      let json = csvToJson.getJsonFromCsv(`${dir}/${file}`);
      //Salva o arquivo json de mensalidade
      fs.writeFileSync(`${dirJson}/${file.replace('.csv', '')}.json`, JSON.stringify(json), 'utf-8')
    });


    connection.end();
    return results;
  } catch (error) {
    console.error('Erro:', error);
  }
}

main()
  .then(() => {
    const fileList = fs.readdirSync(dirJson)
    fileList.forEach(file => {
      const mensalidades = JSON.parse(fs.readFileSync(`${dirJson}/${file}`, "utf8"));
      const idContrato = getIdContratante(file);
      const formatedMensalidade = mensalidades.map(mensalidade => {
        const dados = buscarIdTitularEIdPorCPF(mensalidade.CPF)
        return ({
          ...dados,
          valor: mensalidade.ValorEvento.replace(',','.'),
          nomePaciente: mensalidade.NomePaciente,
          CPF: mensalidade.CPF,
          data: data,
          usuario: user,
          contratante: idContrato
        })
      })
      fs.writeFileSync(`${dirJsonFormatado}/${file}`, JSON.stringify(formatedMensalidade), 'utf-8')
    })
    const fileListFormatado = fs.readdirSync(`${dirJsonFormatado}`)
    fileListFormatado.forEach(file => {
      const mensalidadesFormatadas = JSON.parse(fs.readFileSync(`${dirJsonFormatado}/${file}`, "utf8"));
      const consultas = [];
      mensalidadesFormatadas.forEach((item) => {
        const consulta = `INSERT INTO unimeduberlandia_debitos(id, idtitular, iddependente, conta, valor, isadesao, implantacao, data, usuario, contratante, retroativo) VALUES (null,'${item.idtitular}','${item.iddependente}','${item.conta}','${item.valor}',0,0.00,'${item.data}','${item.usuario}','${item.contratante}',0);`;
        consultas.push(consulta);
      });
      fs.writeFileSync(`./${dirSQL}/${file}.sql`, consultas.join('\n'))
    })
  })
