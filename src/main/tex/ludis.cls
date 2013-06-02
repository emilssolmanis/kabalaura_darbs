\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{ludis}[2012/10/08 LU diplomdarbu stils]

\newcommand{\@spacingsize}{1.5}
\DeclareOption{singlespacing}{\renewcommand{\@spacingsize}{1}}
\newcommand{\@normmargins}{0}
\DeclareOption{latexmargins}{\renewcommand{\@normmargins}{1}}

\newcommand{\@fontsize}{2}
\DeclareOption{12pt}{\renewcommand{\@fontsize}{2}}
\DeclareOption{11pt}{\renewcommand{\@fontsize}{1}}
\DeclareOption{10pt}{\renewcommand{\@fontsize}{0}}

\DeclareOption*{%
	\PassOptionsToClass{\CurrentOption}{report}
}

\ProcessOptions\relax

\if \@fontsize 1
	\PassOptionsToClass{11pt}{report}
\else
	\if \@fontsize 0
		\PassOptionsToClass{10pt}{report}
	\else
		\PassOptionsToClass{12pt}{report}
	\fi
\fi

\LoadClass[a4paper]{report}
\RequirePackage{amsmath}
\RequirePackage{amssymb}
\RequirePackage{amsthm}
\RequirePackage{amsfonts}
\RequirePackage{titlesec}

% Theorems
\numberwithin{equation}{section}
\renewcommand{\theequation}{\thesection\arabic{equation}}
\theoremstyle{definition}
\newtheorem{definicija}{Defin\={\i}cija}%
\newtheorem{pienemums}{Pie\c{n}\={e}mums}%
\newtheorem{piemers}{Piem\={e}rs}%
\theoremstyle{plain}
\newtheorem{teorema}{Teor\={e}ma}%
\newtheorem{apgalvojums}[teorema]{Apgalvojums}%
\newtheorem{lemma}[teorema]{Lemma}%
\newtheorem{sekas}[teorema]{Sekas}%
\newtheorem{piezime}[teorema]{Piez\={\i}me}
\renewcommand\proofname{Pier\={a}d\={\i}jums}
\newenvironment{pieradijums}{\begin{proof}}{\end{proof}}

% Formating
\RequirePackage[left=30mm,top=20mm,right=20mm,bottom=20mm]{geometry}

\renewcommand{\baselinestretch}{\@spacingsize}
% LaTeX has problems with \bfseries\scshape, the only font I've found with
% reasonable unicode support that supports this so far is Linux Libertine.
\renewcommand{\@makechapterhead}[1]{%
	\vspace*{0 pt}%
	{\setlength{\parindent}{1cm} \raggedright \normalfont
        \bfseries\scshape\fontsize{14}{0}\selectfont
		\ifnum \value{secnumdepth}>1
   			\thechapter\ %
		\fi
		#1\par\nobreak\vspace{14 pt}}}

\titleformat{\section}{\bfseries\fontsize{14}{7}\selectfont}{\thesection}{1em}{}
\titleformat{\subsection}{\bfseries\fontsize{14}{7}\selectfont}{\thesubsection}{1em}{}

\RequirePackage{caption}
\captionsetup[figure]{labelfont=it,textfont=bf}

% Toc
\let\@origtoc\tableofcontents
\renewcommand\tableofcontents{%
	\renewcommand{\baselinestretch}{1}
	\@origtoc
    \renewcommand{\baselinestretch}{\@spacingsize}
    }
    
% Bibliography
\newcommand{\literatura}[1]{%
	\newpage
	\renewcommand{\baselinestretch}{1}
	\bibliographystyle{ludis}
	\renewcommand{\bibname}{Izmantot\={a} literat\={u}ra un avoti}
	\addcontentsline{toc}{chapter}{Izmantot\={a} literat\={u}ra un avoti}\bibliography{#1}
	\renewcommand{\baselinestretch}{\@spacingsize}
}

% New commands
\newcommand{\@keywordname}{Keywords:}
\newcommand{\keywords}[1]{\par\addvspace\baselineskip\noindent\@keywordname\enspace\ignorespaces#1}
\newenvironment{abstract-en}{%
	\renewcommand{\@keywordname}{Keywords:}
	\renewcommand{\abstractname}{Abstract}
	\begin{abstract}}{\end{abstract}}
\newenvironment{abstract-lv}{%
	\renewcommand{\@keywordname}{Atsl\={e}gas v\={a}rdi:}
	\renewcommand{\abstractname}{Anot\={a}cija}
	\begin{abstract}}{\end{abstract}}

% Special sections
\newcommand{\specnodala}[1]{\newpage\addcontentsline{toc}{chapter}{#1}\section*{#1}}

% Titlepage and dokumentara lapa
\newcommand{\autors}[1]{\author{#1}}
\newcommand{\@nosaukumsLB}{0}
\newcommand{\nosaukums}[1]{%
	\title{#1}
	\if \@nosaukumsLB 0
		\renewcommand{\@nosaukumsLB}{\MakeUppercase{\@title}}
	\fi	
}
\newcommand{\NOSAUKUMS}[1]{\renewcommand{\@nosaukumsLB}{#1}}
\newcommand{\gads}[1]{\date{#1}}
\newcommand{\universityname}{Latvijas Universit\={a}te}
\newcommand{\universityshort}{LU}
\newcommand{\@fakultate}{}
\newcommand{\@fakultateD}{}
\newcommand{\fakultate}[1]{%
	\renewcommand{\@fakultate}{#1 fakult\={a}te}
	\renewcommand{\@fakultateD}{#1 fakult\={a}t\={e}}}
\newcommand{\@nodala}{}
\newcommand{\@nodalaD}{}
\newcommand{\nodala}[1]{%
	\renewcommand{\@nodala}{#1 noda\c{l}a}
	\renewcommand{\@nodalaD}{#1 noda\c{l}\={a}}}
\newcommand{\@vieta}{}
\newcommand{\vieta}[1]{\renewcommand{\@vieta}{#1}}
\newcommand{\@darbaveids}{}
\newcommand{\@darbaveidsLB}{0}
\newcommand{\darbaveids}[1]{%
	\renewcommand{\@darbaveids}{#1}
	\if \@darbaveidsLB 0
		\renewcommand{\@darbaveidsLB}{\MakeUppercase{\@darbaveids}}
	\fi
}
\newcommand{\DARBAVEIDS}[1]{\renewcommand{\@darbaveidsLB}{#1}}
\newcommand{\@studapl}{}
\newcommand{\studapl}[1]{\renewcommand{\@studapl}{#1}}
\newcommand{\@vaditajs}{}
\newcommand{\vaditajs}[1]{\renewcommand{\@vaditajs}{#1}}
\newcommand{\@recenzents}{}
\newcommand{\recenzents}[1]{\renewcommand{\@recenzents}{#1}}
\renewcommand{\maketitle}{%
  \renewcommand{\baselinestretch}{1}
  \begin{titlepage}%
  \null\vfil
  \begin{center}%
  	{\large \MakeUppercase{\universityname} \par}%
  	{\large \MakeUppercase{\@fakultate}\par}%
 	{\large \MakeUppercase{\@nodala} \par}%
 	\vskip 5em
    {\large \textbf{\@nosaukumsLB} \par}%
    \vskip 2em
    {\large \@darbaveidsLB\ DARBS\par}% 
   \end{center}\par
   \begin{flushleft}
    \vskip 7em%
    {%
      \begin{tabular}[t]{l}%
       Autors: \textbf{\@author} \\
       Stud. apl. \@studapl \\
       Darba vad\={\i}t\={a}js: \@vaditajs
      \end{tabular}\par}%
      \vskip 1.5em%
    \end{flushleft}
    \vskip 7em%
    {\centering\large \MakeUppercase{\@vieta} \@date\par}
  \vfil\null
  \end{titlepage}%
  \global\let\and\relax
   \renewcommand{\baselinestretch}{\@spacingsize}
}
   
\newcommand{\@doklapa}{%
	\newpage\thispagestyle{empty}
  	\renewcommand{\baselinestretch}{1} 
  
	\@darbaveids\ darbs ``\@title'' izstr\={a}d\={a}ts	\universityshort\ \@fakultateD.\par
	\vskip 1em%
	Ar savu parakstu apliecinu, ka p\={e}t\={\i}jums veikts patst\={a}v\={\i}gi, izmantoti tikai taj\={a} nor\={a}d\={\i}tie inform\={a}cijas avoti un iesniegt\={a} darba elektronisk\={a} kopija atbilst izdrukai.\par
	\vskip 1em
	Autors: \@author\par
	\vskip 2em
	\hspace{4em}\begin{tabular}{cc}%
		\underline{\hspace{10em}}\vspace{-2mm} &
		\underline{\hspace{8em}} \\
		 \tiny{(paraksts)} &
		 \tiny{(datums)}
	\end{tabular}
	\vskip 3em%
	Rekomend\={e}ju / nerekoment\={e}ju darbu aizst\={a}v\={e}\v{s}anai.\par
	Vad\={\i}t\={a}js: \@vaditajs\par
	\vskip 2em
	\hspace{4em}\begin{tabular}{cc}%
		\underline{\hspace{10em}}\vspace{-2mm} &
		\underline{\hspace{8em}} \\
		 \tiny{(paraksts)} &
		 \tiny{(datums)}
	\end{tabular}
	\vskip 1em%
	Recenzents: \@recenzents\par
	\vskip 2em
	\hspace{4	em}\begin{tabular}{cc}%
		\underline{\hspace{10em}}\vspace{-2mm} &
		\underline{\hspace{8em}} \\
		 \tiny{(paraksts)} &
		 \tiny{(datums)}
	\end{tabular}

	\vskip 3em%
	\begin{tabular}{lc}
	Darbs iesniegts \@nodalaD & \underline{\hspace{8em}} \\
	& \tiny{(datums)}
	\end{tabular}\par
	\vskip 1em
	\begin{tabular}{c}%
        Dek\={a}na pilnvarot\={a} persona:
		\underline{\hspace{15em}}\vspace{-2mm} \\
	\end{tabular}
	\vskip 5em%
	Darbs aizst\={a}v\={e}ts \MakeLowercase{\@darbaveids} gala p\={a}rbaud\={\i}juma komisijas s\={e}d\={e}\par
	\vskip 1em%
	\begin{tabular}{cl}\underline{\hspace{8em}}\vspace{-2mm} & 	 prot.\ Nr. \underline{\hspace{4em}}, v\={e}rt\={e}jums\underline{\hspace{4em}} \\
	 \tiny{(datums)} & \end{tabular}\par
	\vskip 1em%
	\begin{tabular}{lcc}
	Komisijas sekret\={a}rs/-e: & 
	\underline{\hspace{8em}}\vspace{-2mm} &
	\underline{\hspace{8em}} \\
	& \tiny{(V\={a}rds, Uzv\={a}rds)} & \tiny{(paraksts)} 
	\end{tabular}
}   

\AtEndDocument{\@doklapa}