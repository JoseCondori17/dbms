#!/usr/bin/env python3
"""
Configuración rápida para Kaggle (opcional)
"""
import os
import json
from pathlib import Path

def setup_kaggle_mock():
    """Create a mock Kaggle configuration for testing"""
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(exist_ok=True)
    
    # Create a mock kaggle.json file
    mock_config = {
        "username": "test_user",
        "key": "test_key_placeholder"
    }
    
    kaggle_file = kaggle_dir / "kaggle.json"
    
    if not kaggle_file.exists():
        with open(kaggle_file, 'w') as f:
            json.dump(mock_config, f)
        
        # Set proper permissions (only owner can read/write)
        os.chmod(kaggle_file, 0o600)
        
        print(f"✅ Mock kaggle.json creado en: {kaggle_file}")
        print("⚠️  IMPORTANTE: Este es un archivo de configuración mock para testing")
        print("⚠️  Para usar datasets reales de Kaggle:")
        print("   1. Ve a https://www.kaggle.com/account")
        print("   2. Crea un nuevo API token")
        print("   3. Descarga kaggle.json")
        print(f"   4. Reemplaza el archivo en: {kaggle_file}")
        return True
    else:
        print(f"✅ kaggle.json ya existe en: {kaggle_file}")
        return True

def check_kaggle_setup():
    """Check if Kaggle is properly configured"""
    try:
        import kaggle
        print("✅ Kaggle está configurado correctamente")
        return True
    except ImportError:
        print("❌ Kaggle no está instalado")
        return False
    except Exception as e:
        print(f"⚠️  Kaggle instalado pero no configurado: {e}")
        return False

def main():
    """Main function"""
    print("=== Configuración de Kaggle (Opcional) ===")
    print()
    
    if check_kaggle_setup():
        print("Kaggle ya está configurado y listo para usar")
        return 0
    
    print("Configurando Kaggle con credenciales mock...")
    if setup_kaggle_mock():
        print()
        print("✅ Configuración mock completada")
        print("El sistema puede ejecutarse sin problemas")
        print("Para usar datasets reales, configura credenciales reales de Kaggle")
        return 0
    else:
        print("❌ Error en la configuración")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
