// Anúncio Form ML - JavaScript
class MLAnuncioForm {
    constructor() {
        this.currentSectionIndex = 0;
        this.sections = document.querySelectorAll(".form-section");
        this.navIndicators = document.querySelectorAll(".nav-indicators .indicator");
        this.init();
    }

    init() {
        this.showSection(this.currentSectionIndex);
        this.setupEventListeners();
        this.updateCharCount();
        this.checkInitialOtimizacaoIA();
    }

    setupEventListeners() {
        document.getElementById("titulo").addEventListener("input", () => this.updateCharCount());
        document.getElementById("imageInput").addEventListener("change", (event) => this.handleImageUpload(event));
        document.querySelector(".upload-area").addEventListener("click", () => document.getElementById("imageInput").click());
        document.querySelector(".ai-header").addEventListener("click", () => this.toggleAI());

        // Submit do formulário
        document.getElementById("anuncioForm").addEventListener("submit", (event) => this.handleSubmit(event));
    }

    showSection(index) {
        this.sections.forEach((section, i) => {
            if (i === index) {
                section.classList.add("active");
            } else {
                section.classList.remove("active");
            }
        });
        this.navIndicators.forEach((indicator, i) => {
            if (i === index) {
                indicator.classList.add("active");
            } else {
                indicator.classList.remove("active");
            }
        });
        this.updateNavigationButtons();
    }

    nextSection() {
        if (this.currentSectionIndex < this.sections.length - 1) {
            this.currentSectionIndex++;
            this.showSection(this.currentSectionIndex);
        }
    }

    previousSection() {
        if (this.currentSectionIndex > 0) {
            this.currentSectionIndex--;
            this.showSection(this.currentSectionIndex);
        }
    }

    updateNavigationButtons() {
        document.getElementById("prevBtn").style.display = this.currentSectionIndex === 0 ? "none" : "inline-flex";
        const nextBtn = document.getElementById("nextBtn");
        if (this.currentSectionIndex === this.sections.length - 1) {
            nextBtn.style.display = "none";
        } else {
            nextBtn.style.display = "inline-flex";
            nextBtn.innerHTML = `Próximo <i class="ri-arrow-right-line"></i>`;
        }
    }

    updateCharCount() {
        const tituloInput = document.getElementById("titulo");
        const charCountSpan = document.querySelector(".char-count");
        if (tituloInput && charCountSpan) {
            charCountSpan.textContent = `${tituloInput.value.length}/60 caracteres`;
        }
    }

    handleImageUpload(event) {
        const files = event.target.files;
        const previewContainer = document.getElementById("imagesPreview");
        if (!previewContainer) return;

        for (const file of files) {
            if (file.type.startsWith("image/")) {
                const reader = new FileReader();
                reader.onload = (e) => {
                    const imgItem = document.createElement("div");
                    imgItem.className = "image-item";
                    imgItem.innerHTML = `
                        <img src="${e.target.result}" alt="preview">
                        <button type="button" class="remove-btn"><i class="ri-close-line"></i></button>
                    `;
                    imgItem.querySelector(".remove-btn").addEventListener("click", () => imgItem.remove());
                    previewContainer.appendChild(imgItem);
                };
                reader.readAsDataURL(file);
            }
        }
    }

    toggleAI() {
        const aiAssistant = document.getElementById("aiAssistant");
        aiAssistant.classList.toggle("open");
        const aiContent = document.getElementById("aiContent");
        aiContent.style.maxHeight = aiAssistant.classList.contains("open") ? aiContent.scrollHeight + "px" : "0";
    }

    async handleSubmit(event) {
        event.preventDefault();
        this.showLoading(true);

        const form = event.target;
        const formData = new FormData(form);
        const data = Object.fromEntries(formData.entries());

        data.usar_ia = document.getElementById("usarIA").checked;
        data.publicar_imediatamente = document.getElementById("publicarImediatamente").checked;

        // Coletar imagens (simplificado, em um ambiente real, você enviaria para um serviço de upload)
        const images = [];
        document.querySelectorAll("#imagesPreview img").forEach(img => images.push(img.src));
        data.urls_imagens = JSON.stringify(images);
        data.main_picture_url = images.length > 0 ? images[0] : "";

        const idAnuncio = window.location.pathname.split("/")[4]; // Extrai ID da URL se for edição
        const method = idAnuncio ? "POST" : "POST"; // Flask usa POST para PUT/PATCH via form
        const url = idAnuncio ? `/vendas/ml/anuncio/${idAnuncio}/editar` : 
                                 `/vendas/ml/anuncio/novo`;

        try {
            const response = await fetch(url, {
                method: method,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });
            const result = await response.json();

            if (result.success) {
                this.showNotification(result.message);
                if (!idAnuncio) {
                    // Redirecionar para a página de edição ou lista após a criação
                    window.location.href = 
                        `/vendas/ml/anuncio/${result.new_id || data.id_anuncio}/editar`;
                } else {
                    window.location.reload();
                }
            } else {
                this.showNotification(result.message, "error");
            }
        } catch (error) {
            console.error("Erro ao salvar anúncio:", error);
            this.showNotification("Erro ao salvar anúncio", "error");
        } finally {
            this.showLoading(false);
        }
    }

    checkInitialOtimizacaoIA() {
        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.get("otimizar_ia") === "true") {
            this.toggleAI();
            this.showNotification("Assistente IA ativado para otimização!", "info");
        }
    }

    showNotification(message, type = 'success') {
        const notification = document.getElementById('notification');
        const messageEl = document.getElementById('notification-message');
        
        if (notification && messageEl) {
            messageEl.textContent = message;
            notification.className = `notification ${type}`;
            notification.classList.add('show');
            
            setTimeout(() => {
                notification.classList.remove('show');
            }, 3000);
        }
    }

    showLoading(show = true) {
        const overlay = document.getElementById('loadingOverlay');
        if (overlay) {
            overlay.style.display = show ? 'flex' : 'none';
        }
    }
}

window.mlAnuncioForm = new MLAnuncioForm();

// Funções globais para interação com templates
function voltarLista() {
    window.location.href = '/vendas/ml/anuncios';
}

function previewAnuncio() {
    const modal = document.getElementById("previewModal");
    const content = document.getElementById("previewContent");
    if (modal && content) {
        // Simular a criação de um preview com os dados do formulário
        const titulo = document.getElementById("titulo").value;
        const descricao = document.getElementById("descricao").value;
        const preco = document.getElementById("preco").value;
        const imagens = Array.from(document.querySelectorAll("#imagesPreview img")).map(img => img.src);

        content.innerHTML = `
            <div class="preview-content">
                <h3>${titulo || "Título do Anúncio"}</h3>
                <div class="preview-images">
                    ${imagens.map(img => `<img src="${img}" style="width: 100px; height: 100px; object-fit: cover; margin: 5px; border-radius: 8px;">`).join("")}
                </div>
                <p><strong>Preço:</strong> R$ ${parseFloat(preco || 0).toFixed(2)}</p>
                <h4>Descrição:</h4>
                <div class="preview-description">${descricao || "Nenhuma descrição fornecida."}</div>
            </div>
        `;
        modal.style.display = "flex";
    }
}

function fecharPreview() {
    document.getElementById("previewModal").style.display = "none";
}

async function otimizarTitulo() {
    mlAnuncioForm.showLoading(true);
    const tituloInput = document.getElementById("titulo");
    const categoriaInput = document.getElementById("categoria");
    try {
        const response = await fetch('/vendas/ml/api/otimizar-titulo', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                titulo: tituloInput.value,
                categoria: categoriaInput.value
            })
        });
        const result = await response.json();
        if (result.titulo_otimizado) {
            tituloInput.value = result.titulo_otimizado;
            mlAnuncioForm.updateCharCount();
            mlAnuncioForm.showNotification("Título otimizado com IA!");
        } else {
            mlAnuncioForm.showNotification("Não foi possível otimizar o título.", "warning");
        }
    } catch (error) {
        console.error("Erro ao otimizar título:", error);
        mlAnuncioForm.showNotification("Erro ao otimizar título com IA", "error");
    } finally {
        mlAnuncioForm.showLoading(false);
    }
}

async function gerarDescricao() {
    mlAnuncioForm.showLoading(true);
    const descricaoTextarea = document.getElementById("descricao");
    const titulo = document.getElementById("titulo").value;
    const categoria = document.getElementById("categoria").value;
    const preco = document.getElementById("preco").value;
    // Adicionar mais campos conforme necessário para a IA
    try {
        const response = await fetch('/vendas/ml/api/gerar-descricao', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                titulo: titulo,
                categoria: categoria,
                preco: preco
            })
        });
        const result = await response.json();
        if (result.descricao) {
            descricaoTextarea.value = result.descricao;
            mlAnuncioForm.showNotification("Descrição gerada com IA!");
        } else {
            mlAnuncioForm.showNotification("Não foi possível gerar a descrição.", "warning");
        }
    } catch (error) {
        console.error("Erro ao gerar descrição:", error);
        mlAnuncioForm.showNotification("Erro ao gerar descrição com IA", "error");
    } finally {
        mlAnuncioForm.showLoading(false);
    }
}

async function sugerirPreco() {
    mlAnuncioForm.showLoading(true);
    const precoInput = document.getElementById("preco");
    const categoria = document.getElementById("categoria").value;
    // Em um cenário real, você passaria mais dados do produto para a IA
    try {
        const response = await fetch('/vendas/ml/api/analisar-concorrencia', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                categoria: categoria,
                palavra_chave: document.getElementById("titulo").value.split(" ")[0] // Exemplo simples
            })
        });
        const result = await response.json();
        if (result.sugestao_preco) {
            precoInput.value = result.sugestao_preco;
            mlAnuncioForm.showNotification(`Preço sugerido pela IA: R$ ${result.sugestao_preco.toFixed(2)}`);
        } else {
            mlAnuncioForm.showNotification("Não foi possível sugerir um preço.", "warning");
        }
    } catch (error) {
        console.error("Erro ao sugerir preço:", error);
        mlAnuncioForm.showNotification("Erro ao sugerir preço com IA", "error");
    } finally {
        mlAnuncioForm.showLoading(false);
    }
}

async function analisarConcorrencia() {
    mlAnuncioForm.showLoading(true);
    const aiSuggestionsDiv = document.getElementById("aiSuggestions");
    const categoria = document.getElementById("categoria").value;
    const titulo = document.getElementById("titulo").value;

    try {
        const response = await fetch('/vendas/ml/api/analisar-concorrencia', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                categoria: categoria,
                palavra_chave: titulo.split(" ")[0]
            })
        });
        const result = await response.json();
        if (result) {
            aiSuggestionsDiv.innerHTML = `
                <h4>Análise de Concorrência:</h4>
                <p>Preço Médio: R$ ${result.preco_medio.toFixed(2)}</p>
                <p>Preço Sugerido: R$ ${result.sugestao_preco.toFixed(2)}</p>
                <p>Concorrentes Ativos: ${result.concorrentes_ativos}</p>
                <p>Palavras-chave Populares: ${result.palavras_chave_populares.join(", ")}</p>
            `;
            mlAnuncioForm.showNotification("Análise de concorrência concluída!");
        } else {
            aiSuggestionsDiv.innerHTML = `<p>Não foi possível realizar a análise de concorrência.</p>`;
            mlAnuncioForm.showNotification("Não foi possível analisar a concorrência.", "warning");
        }
    } catch (error) {
        console.error("Erro ao analisar concorrência:", error);
        mlAnuncioForm.showNotification("Erro ao analisar concorrência com IA", "error");
    } finally {
        mlAnuncioForm.showLoading(false);
    }
}

function salvarRascunho() {
    mlAnuncioForm.showNotification("Salvando rascunho...", "info");
    // Implementar lógica de salvar rascunho (pode ser um submit com um campo oculto)
    document.getElementById("anuncioForm").submit();
}

async function melhorarDescricao() {
    mlAnuncioForm.showLoading(true);
    const descricaoTextarea = document.getElementById("descricao");
    const titulo = document.getElementById("titulo").value;
    const categoria = document.getElementById("categoria").value;
    const preco = document.getElementById("preco").value;

    try {
        const response = await fetch('/vendas/ml/api/gerar-descricao', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                titulo: titulo,
                categoria: categoria,
                preco: preco,
                descricao_existente: descricaoTextarea.value // Passa a descrição atual para a IA melhorar
            })
        });
        const result = await response.json();
        if (result.descricao) {
            descricaoTextarea.value = result.descricao;
            mlAnuncioForm.showNotification("Descrição melhorada com IA!");
        } else {
            mlAnuncioForm.showNotification("Não foi possível melhorar a descrição.", "warning");
        }
    } catch (error) {
        console.error("Erro ao melhorar descrição:", error);
        mlAnuncioForm.showNotification("Erro ao melhorar descrição com IA", "error");
    } finally {
        mlAnuncioForm.showLoading(false);
    }
}

// Adicionar estilos CSS para elementos dinâmicos
const dynamicStyles = `
    .preview-content {
        padding: 20px;
        background: var(--bg-light);
        border-radius: var(--border-radius-sm);
        color: var(--text-light-contrast);
    }
    .dark-theme .preview-content {
        background: var(--bg-dark);
        color: var(--text-dark);
    }
    .preview-content h3 {
        color: var(--primary-bright);
        margin-bottom: 15px;
    }
    .dark-theme .preview-content h3 {
        color: var(--primary-dark);
    }
    .preview-images {
        display: flex;
        flex-wrap: wrap;
        margin-bottom: 15px;
    }
    .preview-description {
        border-top: 1px solid rgba(0, 208, 255, 0.1);
        padding-top: 15px;
        margin-top: 15px;
        line-height: 1.6;
    }
    .dark-theme .preview-description {
        border-top-color: rgba(0, 240, 255, 0.1);
    }
`;

const styleSheet = document.createElement('style');
styleSheet.textContent = dynamicStyles;
document.head.appendChild(styleSheet);
