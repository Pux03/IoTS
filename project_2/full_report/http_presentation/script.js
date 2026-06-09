const slides = Array.from(document.querySelectorAll('.slide'));
const outlineItems = Array.from(document.querySelectorAll('.outline__item'));
const progressBar = document.getElementById('progress-bar');
const slideIndex = document.getElementById('slide-index');
const prevButton = document.getElementById('prev-slide');
const nextButton = document.getElementById('next-slide');

let currentSlide = 0;
let wheelLock = false;

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function renderSlide(index) {
  currentSlide = clamp(index, 0, slides.length - 1);

  slides.forEach((slide, slideIdx) => {
    slide.classList.toggle('active', slideIdx === currentSlide);
  });

  outlineItems.forEach((item, itemIdx) => {
    item.classList.toggle('active', itemIdx === currentSlide);
  });

  const progressPercent = ((currentSlide + 1) / slides.length) * 100;
  progressBar.style.width = `${progressPercent}%`;
  slideIndex.textContent = String(currentSlide + 1).padStart(2, '0');

  prevButton.disabled = currentSlide === 0;
  nextButton.disabled = currentSlide === slides.length - 1;
}

function goToSlide(index) {
  renderSlide(index);
}

function goNext() {
  goToSlide(currentSlide + 1);
}

function goPrev() {
  goToSlide(currentSlide - 1);
}

outlineItems.forEach((item, index) => {
  item.addEventListener('click', () => goToSlide(index));
});

prevButton.addEventListener('click', goPrev);
nextButton.addEventListener('click', goNext);

document.addEventListener('keydown', (event) => {
  if (event.key === 'ArrowRight' || event.key === 'PageDown' || event.key === ' ') {
    event.preventDefault();
    goNext();
  }
  if (event.key === 'ArrowLeft' || event.key === 'PageUp') {
    event.preventDefault();
    goPrev();
  }
  if (event.key === 'Home') {
    event.preventDefault();
    goToSlide(0);
  }
  if (event.key === 'End') {
    event.preventDefault();
    goToSlide(slides.length - 1);
  }
});

document.addEventListener(
  'wheel',
  (event) => {
    if (window.innerWidth <= 1200 || wheelLock) {
      return;
    }

    if (Math.abs(event.deltaY) < 18) {
      return;
    }

    wheelLock = true;
    if (event.deltaY > 0) {
      goNext();
    } else {
      goPrev();
    }

    setTimeout(() => {
      wheelLock = false;
    }, 360);
  },
  { passive: true }
);

renderSlide(0);
