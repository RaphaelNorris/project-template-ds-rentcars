import os
import sys
import json
import requests
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime


class ExceptionHandler:
    @staticmethod
    def handle_exception(exception, context=""):
        print(f"Error in {context}: {str(exception)}")


class DocumentationGenerator:
    def __init__(self):
        load_dotenv()
        self.token = os.getenv("FLOW_TOKEN")
        if not self.token:
            raise Exception("❌ FLOW_TOKEN não encontrado no .env")

        self.base_url = "https://flow.com/channels-service"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def create_payload(self, message: str):
        return {
            "content": [{"type": "text/plain", "value": message}],
            "model": {
                "name": "claude37sonnet",
                "provider": "amazon-bedrock",
                "modelSettings": []
            },
            "agent": "chat-with-docs",
            "sources": [],
            "connectors": [],
            "operation": "new-question"
        }

    def chat_completion(self, message: str) -> Optional[str]:
        try:
            url_initial = f"{self.base_url}/v2/chat/messages"
            response_initial = requests.post(url_initial, headers=self.headers, json=self.create_payload(message))

            if response_initial.status_code != 200:
                ExceptionHandler.handle_exception(Exception(f"Erro inicial: {response_initial.text}"), context="chat_completion")
                return None

            chat_id = json.loads(response_initial.text.strip().split("\n")[0]).get("chatId")
            if not chat_id:
                print("❌ chatId não encontrado.")
                return None

            url_second = f"{self.base_url}/v1/chat/{chat_id}/messages"
            response_second = requests.get(url_second, headers=self.headers)

            if response_second.status_code != 200:
                ExceptionHandler.handle_exception(Exception(f"Erro segunda requisição: {response_second.status_code}"), context="chat_completion")
                return None

            messages = response_second.json().get("messages", [])
            result = ""

            for msg in messages:
                for item in msg.get("metadata", {}).get("content", []):
                    if item.get("author") == "assistant":
                        result += self.clean_code_blocks(item.get("value", ""))

            return result

        except Exception as e:
            ExceptionHandler.handle_exception(e, context="chat_completion")
            return None

    def clean_code_blocks(self, text):
        import re
        return re.sub(r'```[a-zA-Z0-9]*\n|```\n|```[a-zA-Z0-9]*|```', '', text)


def build_prompt(file_path, relative_path, content):
    return f"""# system:
                        Você é um engenheiro de dados senior experiente e está analisando um código do SQL Server 2019, para fazer uma documentação funcional e técnica.

                        # user:
                        Você deverá fazer a análise do código <codigo> abaixo para documentação.

                        O caminho relativo do arquivo é {relative_path}.

                        <codigo>
                        {content}
                        </codigo>

                        Siga o padrão abaixo para a documentação:

                        0. **Índice**: Crie um índice com os tópicos a serem abordados na documentação.
                        1. **Resumo do Script**: Uma visão geral do que o script realiza.
                        2. **Objetivo Funcional**: O que se espera alcançar com a execução deste script.
                        3. **Estrutura Técnica**: Como o script está estruturado, incluindo a lógica aplicada.
                        4. **Descrição da Base de Dados**: Com o Data Base, Schema, Tabelas e Colunas utilizadas.
                        5. **Regra de Negócio**: Quais regras de negócio estão sendo aplicadas no script.
                        6. **Fluxo de Dados**: Como os dados fluem através do script, incluindo qualquer transformação aplicada.
                        7. **Exemplos de Uso**: Situações em que este script pode ser utilizado.
                        8. **Avaliação de Performance**: Considerações sobre performance, otimizações ou potenciais gargalos.
                        9. **Tratamento de Erros**: Estratégias de tratamento de erros no script.
                        10. **Dependências**: Identificação de quaisquer dependências externas.
                        11. **Ambiente de Execução**: Informações sobre o ambiente em que o script deve ser executado.
                        12. **Histórico de Alterações**: Registro de alterações feitas no script ao longo do tempo.
                        13. **Testes e Validação**: Métodos de teste e validação do script.
                        14. **Considerações de Segurança**: Aspectos de segurança no script.
                        15. **Versionamento**: Notas sobre gerenciamento de versão do script.
                        16. **Conclusões**: Resumo final sobre o script, incluindo pontos fortes e fracos.
                        17. **Recomendações**: Sugestões para melhorias ou considerações adicionais.

                        Ao final, considerando todo o entendimento relacionado aos scripts, apresente:
                        1 - explicação em relação às reponsabilidades da classe.  
                        2 - detalhe os padrões técnicos do script (exemplo: ORM, POO, SOLID, etc).

                        Sua resposta deve seguir o padrão expecificado na tag <formato> abaixo:

                        <formato>
                        [Caminho do arquivo]:[método/atributo]: [explicação],[parametro1]: [explicação], [parametro3]: [explicação], [parametroN]: [explicação]Retorno: [explicação] Classe: [Responsabilidade da classe] Padrões técnicos: [Padrões técnicos]
                        </formato>

                        Responda apenas o que foi solicitado no formato especificado acima. Não inclua justificativas, explicações adicionais ou blocos de código como "```".
                        """


def process_directory(source_dir: str, output_dir: str):
    doc_gen = DocumentationGenerator()
    count = 0

    os.makedirs(output_dir, exist_ok=True)

    for root, dirs, files in os.walk(source_dir):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for file in files:
            if file.startswith('.') or not file.lower().endswith(('.py', '.java', '.ts', '.js')):
                continue

            abs_path = os.path.join(root, file)
            rel_path = os.path.relpath(abs_path, source_dir)
            filename_without_ext = os.path.splitext(file)[0]
            output_file_path = os.path.join(output_dir, f"{filename_without_ext}.md")

            try:
                with open(abs_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            except UnicodeDecodeError:
                with open(abs_path, 'r', encoding='latin-1') as f:
                    content = f.read()

            if not content.strip():
                continue

            print(f"📄 Documentando: {rel_path}")
            prompt = build_prompt(abs_path, rel_path, content)
            result = doc_gen.chat_completion(prompt)

            if result:
                with open(output_file_path, 'w', encoding='utf-8') as f:
                    f.write(f"# Documentação de {file}\n")
                    f.write(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n")
                    f.write(result)


                print(f"✅ Salvo em: {output_file_path}")
                count += 1

    print(f"\n🎯 Documentação gerada para {count} arquivo(s).")


if __name__ == "__main__":
    print("📂 Informe o caminho da pasta com os scripts:")
    source_folder = input(">> ").strip()

    print("📁 Informe a pasta onde deseja salvar os arquivos .md:")
    output_dir = input(">> ").strip()

    if not os.path.isdir(source_folder):
        print(f"❌ Diretório de origem inválido: {source_folder}")
        sys.exit(1)

    process_directory(source_folder, output_dir)
