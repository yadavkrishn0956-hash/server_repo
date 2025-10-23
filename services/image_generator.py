import numpy as np
from PIL import Image, ImageDraw, ImageFont
import io
import zipfile
import random
from typing import List, Tuple, Dict, Any
import math

class ImageDataGenerator:
    """Generate synthetic 32x32 pixel images with various patterns"""
    
    def __init__(self):
        self.image_size = (32, 32)
        self.channels = 3  # RGB
    
    def generate_noise_image(self) -> np.ndarray:
        """Generate random noise image"""
        return np.random.randint(0, 256, (*self.image_size, self.channels), dtype=np.uint8)
    
    def generate_gradient_image(self) -> np.ndarray:
        """Generate gradient pattern image"""
        img = np.zeros((*self.image_size, self.channels), dtype=np.uint8)
        
        # Create gradient
        for i in range(self.image_size[0]):
            for j in range(self.image_size[1]):
                # Horizontal gradient
                value = int((i / self.image_size[0]) * 255)
                img[i, j] = [value, value, 255 - value]
        
        return img
    
    def generate_geometric_pattern(self) -> np.ndarray:
        """Generate geometric patterns (circles, squares, triangles)"""
        img = Image.new('RGB', self.image_size, color=(0, 0, 0))
        draw = ImageDraw.Draw(img)
        
        # Random background color
        bg_color = tuple(np.random.randint(0, 128, 3))
        img = Image.new('RGB', self.image_size, color=bg_color)
        draw = ImageDraw.Draw(img)
        
        # Random shape color
        shape_color = tuple(np.random.randint(128, 256, 3))
        
        # Choose random shape
        shape_type = random.choice(['circle', 'rectangle', 'triangle'])
        
        if shape_type == 'circle':
            center = (16, 16)
            radius = random.randint(5, 12)
            draw.ellipse([center[0]-radius, center[1]-radius, 
                         center[0]+radius, center[1]+radius], fill=shape_color)
        
        elif shape_type == 'rectangle':
            x1, y1 = random.randint(2, 10), random.randint(2, 10)
            x2, y2 = random.randint(22, 30), random.randint(22, 30)
            draw.rectangle([x1, y1, x2, y2], fill=shape_color)
        
        elif shape_type == 'triangle':
            points = [(16, 5), (5, 27), (27, 27)]
            draw.polygon(points, fill=shape_color)
        
        return np.array(img)
    
    def generate_checkerboard_pattern(self) -> np.ndarray:
        """Generate checkerboard pattern"""
        img = np.zeros((*self.image_size, self.channels), dtype=np.uint8)
        
        # Colors for checkerboard
        color1 = np.random.randint(0, 128, 3)
        color2 = np.random.randint(128, 256, 3)
        
        # Checkerboard size
        square_size = random.choice([2, 4, 8])
        
        for i in range(self.image_size[0]):
            for j in range(self.image_size[1]):
                if ((i // square_size) + (j // square_size)) % 2 == 0:
                    img[i, j] = color1
                else:
                    img[i, j] = color2
        
        return img
    
    def generate_spiral_pattern(self) -> np.ndarray:
        """Generate spiral pattern"""
        img = np.zeros((*self.image_size, self.channels), dtype=np.uint8)
        
        center_x, center_y = 16, 16
        
        for i in range(self.image_size[0]):
            for j in range(self.image_size[1]):
                # Calculate distance and angle from center
                dx = i - center_x
                dy = j - center_y
                distance = math.sqrt(dx*dx + dy*dy)
                angle = math.atan2(dy, dx)
                
                # Create spiral pattern
                spiral_value = (angle + distance * 0.3) % (2 * math.pi)
                intensity = int((spiral_value / (2 * math.pi)) * 255)
                
                img[i, j] = [intensity, 255 - intensity, (intensity + 128) % 255]
        
        return img
    
    def generate_texture_pattern(self) -> np.ndarray:
        """Generate texture-like pattern using Perlin noise approximation"""
        img = np.zeros((*self.image_size, self.channels), dtype=np.uint8)
        
        # Simple texture using multiple sine waves
        for i in range(self.image_size[0]):
            for j in range(self.image_size[1]):
                # Combine multiple frequencies
                value1 = math.sin(i * 0.3) * math.cos(j * 0.3)
                value2 = math.sin(i * 0.1) * math.cos(j * 0.1)
                value3 = math.sin(i * 0.5) * math.cos(j * 0.5)
                
                combined = (value1 + value2 * 0.5 + value3 * 0.25)
                intensity = int((combined + 1) * 127.5)  # Normalize to 0-255
                
                # Ensure values stay within uint8 bounds (0-255)
                intensity = max(0, min(255, intensity))
                r = intensity
                g = (intensity + 85) % 256
                b = (intensity + 170) % 256
                
                img[i, j] = [r, g, b]
        
        return img
    
    def generate_single_image(self, pattern_type: str = None) -> np.ndarray:
        """Generate a single image with specified or random pattern"""
        if pattern_type is None:
            pattern_type = random.choice([
                'noise', 'gradient', 'geometric', 'checkerboard', 'spiral', 'texture'
            ])
        
        if pattern_type == 'noise':
            return self.generate_noise_image()
        elif pattern_type == 'gradient':
            return self.generate_gradient_image()
        elif pattern_type == 'geometric':
            return self.generate_geometric_pattern()
        elif pattern_type == 'checkerboard':
            return self.generate_checkerboard_pattern()
        elif pattern_type == 'spiral':
            return self.generate_spiral_pattern()
        elif pattern_type == 'texture':
            return self.generate_texture_pattern()
        else:
            return self.generate_noise_image()
    
    def generate_image_dataset(self, num_images: int, 
                             pattern_distribution: Dict[str, float] = None) -> List[np.ndarray]:
        """Generate a dataset of multiple images"""
        
        if pattern_distribution is None:
            # Default equal distribution
            pattern_distribution = {
                'noise': 0.2,
                'gradient': 0.15,
                'geometric': 0.25,
                'checkerboard': 0.15,
                'spiral': 0.15,
                'texture': 0.1
            }
        
        images = []
        patterns = list(pattern_distribution.keys())
        weights = list(pattern_distribution.values())
        
        for _ in range(num_images):
            pattern = np.random.choice(patterns, p=weights)
            image = self.generate_single_image(pattern)
            images.append(image)
        
        return images
    
    def images_to_zip_bytes(self, images: List[np.ndarray]) -> bytes:
        """Convert list of images to ZIP file bytes"""
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, img_array in enumerate(images):
                # Convert numpy array to PIL Image
                img = Image.fromarray(img_array, 'RGB')
                
                # Save image to bytes
                img_buffer = io.BytesIO()
                img.save(img_buffer, format='PNG')
                img_bytes = img_buffer.getvalue()
                
                # Add to ZIP
                zip_file.writestr(f'image_{i+1:04d}.png', img_bytes)
            
            # Add metadata file
            metadata = {
                'total_images': len(images),
                'image_size': self.image_size,
                'format': 'PNG',
                'channels': self.channels,
                'generated_at': '2024-01-15T10:30:00Z'
            }
            
            import json
            zip_file.writestr('metadata.json', json.dumps(metadata, indent=2))
        
        zip_buffer.seek(0)
        return zip_buffer.getvalue()
    
    def get_image_preview_data(self, images: List[np.ndarray], 
                              sample_size: int = 3) -> Dict[str, Any]:
        """Generate preview data for image dataset"""
        
        # Convert sample images to base64 for frontend display
        import base64
        
        sample_images = images[:sample_size]
        preview_images = []
        
        for img_array in sample_images:
            img = Image.fromarray(img_array, 'RGB')
            img_buffer = io.BytesIO()
            img.save(img_buffer, format='PNG')
            img_base64 = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
            preview_images.append(f'data:image/png;base64,{img_base64}')
        
        # Calculate dataset statistics
        total_size_bytes = sum(img.nbytes for img in images)
        
        return {
            'sample_images': preview_images,
            'total_images': len(images),
            'image_dimensions': f'{self.image_size[0]}x{self.image_size[1]}',
            'channels': self.channels,
            'total_size_mb': round(total_size_bytes / (1024 * 1024), 2),
            'format': 'PNG (in ZIP archive)'
        }

# Global instance
image_generator = ImageDataGenerator()