const btnSubir = document.getElementById("btnSubir");

window.onscroll = function() {

if (document.body.scrollTop > 300 || document.documentElement.scrollTop > 300) {
btnSubir.style.display = "block";
} else {
btnSubir.style.display = "none";
}

};

function subirArriba() {

window.scrollTo({
top: 0,
behavior: "smooth"
});

}

