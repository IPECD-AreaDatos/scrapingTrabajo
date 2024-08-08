             <style>
                    h2 {{
                        font-size: 24px;
                        color: #465c49;
                        text-align: center;
                    }}
                    .container {{
                        width: 100%;
                        background-image: url("cid:fondo");
                        background-size: cover;
                        background-position: center;
                        overflow: hidden; /* Oculta las barras de desplazamiento */
                    }}
                    .container-data-general {{
                        width: 100%;
                        text-align: center;
                        margin-bottom: 20px;
                    }}
                    .data-item-valor-general {{
                        display: inline-flex;
                        justify-content: center;
                        align-items: center;
                        border: 2px solid #465c49;
                        border-radius: 10px;
                        padding: 10px;
                        background-image: url("cid:fondo_cuadros");
                        background-size: cover;
                        background-position: center;
                        overflow: hidden; /* Oculta las barras de desplazamiento */
                        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                        box-sizing: border-box;
                        width: auto;
                        text-align: center;
                        margin: 0 auto;
                        flex-direction: column;
                    }}
                    .container-data-general span {{
                        display: block;
                        width: 100%;
                        font-size: 24px;
                        color: #ffffff;
                        text-align: center;
                        padding: 20px;
                    }}
                    .container-data {{
                        width: 100%;
                        display: flex;
                        flex-direction: column;
                        gap: 20px;
                    }}
                    .data-item-variaciones {{
                        display: block;
                        border: 2px solid #465c49;
                        border-radius: 10px;
                        padding: 10px;
                        text-align: center;
                        background-image: url("cid:fondo_cuadros");
                        background-size: cover;
                        background-position: center;
                        overflow: hidden; /* Oculta las barras de desplazamiento */
                        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                        box-sizing: border-box;
                        margin: 10px;
                        width: 100%;
                        color: #ffffff;
                    }}
                    .data-item-variaciones span {{
                        color: #ffffff;
                    }}
                    .footer {{
                        font-size: 15px;
                        color: #888;
                        margin-top: 20px;
                        text-align: center;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        padding-bottom: 20px;
                    }}
                    .footer img {{
                        margin-right: 20px;
                        max-width: 100px; /* Ajusta el tamaño del logo */
                        height: auto;
                        pointer-events: none;
                        user-select: none;
                    }}
                    .footer-text {{
                        text-align: left;
                    }}
                    @media (max-width: 768px) {{
                        .data-item {{
                            width: 100%;
                        }}
                        .container-footer {{
                            flex-direction: column;
                        }}
                        .footer img {{
                            margin-bottom: 10px;
                            margin-right: 0;
                        }}
                        .footer-text {{
                            text-align: center;
                        }}
                    }}
                    @media (max-width: 480px) {{
                        h2 {{
                            font-size: 18px;
                        }}
                        .data-item-valor-general {{
                            flex-direction: column;
                            padding: 20px;
                            margin-bottom: 20px;
                        }}
                        .data-item-valor-general img {{
                            margin-bottom: 10px;
                            margin-right: 0;
                        }}
                        .container-data {{
                            flex-direction: column;
                            gap: 10px;
                        }}
                        .data-item-variaciones {{
                            margin: 5px;
                            padding: 15px;
                        }}
                    }}
                </style>
            </head>