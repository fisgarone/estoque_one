// importador.js - Versão simplificada e funcional
document.addEventListener('DOMContentLoaded', function() {
    console.log('Importador JS carregado');
    
    // Dropzone functionality
    const dz = document.getElementById('dropzone');
    const fi = document.getElementById('file-input');
    
    if (dz && fi) {
        console.log('Inicializando dropzone...');
        
        dz.addEventListener('click', () => {
            console.log('Dropzone clicada');
            fi.click();
        });
        
        ['dragenter', 'dragover'].forEach(evt => {
            dz.addEventListener(evt, e => {
                e.preventDefault();
                e.stopPropagation();
                dz.classList.add('hover');
                console.log('Arquivo sobre a dropzone');
            });
        });
        
        ['dragleave', 'drop'].forEach(evt => {
            dz.addEventListener(evt, e => {
                e.preventDefault();
                e.stopPropagation();
                dz.classList.remove('hover');
            });
        });
        
        dz.addEventListener('drop', (e) => {
            console.log('Arquivo solto');
            const f = e.dataTransfer.files && e.dataTransfer.files[0];
            if (f && (f.name.toLowerCase().endsWith('.xlsx') || 
                      f.name.toLowerCase().endsWith('.xls') || 
                      f.name.toLowerCase().endsWith('.csv'))) {
                fi.files = e.dataTransfer.files;
                updateDropzoneText(f.name);
                showNotification('Arquivo carregado com sucesso!', 'success');
            } else {
                showNotification('Formato inválido. Use .xlsx, .xls ou .csv', 'error');
            }
        });
        
        fi.addEventListener('change', () => {
            console.log('Arquivo selecionado via input');
            if (fi.files.length) {
                updateDropzoneText(fi.files[0].name);
                showNotification('Arquivo selecionado!', 'success');
            }
        });
        
        function updateDropzoneText(filename) {
            const span = dz.querySelector('.dropzone-filename');
            if (span) {
                span.textContent = filename;
                span.style.fontWeight = '600';
                span.style.color = 'var(--primary-bright)';
            }
        }
    }
    
    // Validação de campos numéricos
    const numericInputs = document.querySelectorAll('input[inputmode="decimal"]');
    numericInputs.forEach(input => {
        input.addEventListener('blur', function() {
            const value = this.value.replace(',', '.');
            if (!isNaN(value) && value.trim() !== '') {
                this.value = parseFloat(value).toFixed(4);
                this.style.borderColor = 'var(--success-color)';
                setTimeout(() => this.style.borderColor = '', 1000);
            } else if (value.trim() !== '') {
                this.style.borderColor = 'var(--error-color)';
            }
        });
    });
});

// Função de notificação global
function showNotification(message, type = 'success') {
    console.log(`Notification [${type}]: ${message}`);
    
    // Usar o sistema do base.html se disponível
    if (typeof window.showNotification === 'function') {
        window.showNotification(message, type);
        return;
    }
    
    // Fallback para desenvolvimento
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 15px 25px;
        border-radius: 12px;
        background: ${type === 'error' ? '#ff3e5f' : type === 'warning' ? '#ffaa00' : '#00c896'};
        color: white;
        box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        z-index: 9999;
        font-family: 'Inter', sans-serif;
    `;
    notification.textContent = message;
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.remove();
    }, 4000);
}