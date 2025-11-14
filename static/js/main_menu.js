document.addEventListener('DOMContentLoaded', function() {
    const mainMenuDiv = document.getElementById('main-menu');
    if (!mainMenuDiv) return;

    // Função para criar um item de menu (principal ou submenu)
    function createMenuItem(item) {
        const menuItem = document.createElement('div');
        menuItem.className = 'menu-item';

        const mainLink = document.createElement('a');
        mainLink.className = 'menu-link';
        mainLink.innerHTML = `<i class="${item.icone || 'ri-question-line'}"></i> <span>${item.nome}</span>`;
        mainLink.href = item.url || '#';
        if (item.url && item.url.startsWith('http')) {
            mainLink.target = '_blank';
            mainLink.rel = 'noopener noreferrer';
        }

        mainLink.onclick = (e) => {
            if (item.submenu && item.submenu.length) {
                e.preventDefault();
                menuItem.classList.toggle('open');
            } else if (item.url) {
                // Navegar para a URL se não houver submenu ou se for um item final
                window.location.href = item.url;
            }
        };

        menuItem.appendChild(mainLink);

        if (item.submenu && item.submenu.length) {
            const submenu = document.createElement('div');
            submenu.className = 'submenu-list';
            item.submenu.forEach(sub => {
                const subLink = document.createElement('a');
                subLink.href = sub.url || '#';
                if (sub.url && sub.url.startsWith('http')) {
                    subLink.target = '_blank';
                    subLink.rel = 'noopener noreferrer';
                }
                subLink.innerHTML = sub.nome;
                subLink.onclick = (e) => {
                    if (sub.url) {
                        window.location.href = sub.url;
                    }
                };
                submenu.appendChild(subLink);
            });
            menuItem.appendChild(submenu);
        }
        return menuItem;
    }

    // Função para carregar o menu via API
    async function loadMenu() {
        try {
            const response = await fetch('/api/menu'); // Endpoint para o menu dinâmico
            const menuData = await response.json();

            menuData.forEach(group => {
                const groupTitle = document.createElement('div');
                groupTitle.className = 'menu-group-title';
                groupTitle.textContent = group.titulo;
                mainMenuDiv.appendChild(groupTitle);

                group.itens.forEach(item => {
                    mainMenuDiv.appendChild(createMenuItem(item));
                });
            });
        } catch (error) {
            console.error('Erro ao carregar o menu:', error);
            // Fallback ou mensagem de erro
            mainMenuDiv.innerHTML = '<p style="color: var(--error-color); padding: 20px;">Erro ao carregar menu.</p>';
        }
    }

    loadMenu();

    // Lógica de tema (já presente no base.html, mas replicada para consistência)
    const themeToggle = document.getElementById('theme-toggle');
    const body = document.body;

    function applyTheme(theme) {
        if (theme === 'dark') {
            body.classList.add('dark-theme');
            themeToggle.innerHTML = '<i class="ri-sun-line"></i>';
        } else {
            body.classList.remove('dark-theme');
            themeToggle.innerHTML = '<i class="ri-contrast-2-fill"></i>';
        }
    }

    // Carregar tema salvo ou usar padrão
    const savedTheme = localStorage.getItem('theme') || 'light';
    applyTheme(savedTheme);

    themeToggle.addEventListener('click', () => {
        const newTheme = body.classList.contains('dark-theme') ? 'light' : 'dark';
        applyTheme(newTheme);
        localStorage.setItem('theme', newTheme);
    });

    // Lógica do menu mobile
    const mobileMenuToggle = document.getElementById('mobile-menu-toggle');
    const sidebar = document.getElementById('sidebar');

    if (mobileMenuToggle && sidebar) {
        mobileMenuToggle.addEventListener('click', () => {
            sidebar.classList.toggle('open');
        });
    }
});

// Funções de notificação e modal (já presentes no base.html, mas replicadas para consistência)
function showNotification(message, type = 'success') {
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

function fecharModalPremium() {
    document.getElementById('premiumModal').style.display = 'none';
}

function exportarDashboard() {
    showNotification('Exportando dashboard...', 'info');
    fecharModalPremium();
}

function compararPeriodos() {
    showNotification('Comparando períodos...', 'info');
    fecharModalPremium();
}

function verDetalhes() {
    window.location.href = '/vendas/ml/relatorios'; // Exemplo de redirecionamento
    fecharModalPremium();
}

