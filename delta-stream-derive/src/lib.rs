use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, parse_quote, Data, DeriveInput, Fields, GenericParam};

#[proc_macro_derive(DeltaState)]
pub fn derive_delta_state(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    if let Err(err) = validate_input(&input) {
        return err.to_compile_error().into();
    }

    let name = &input.ident;
    let mut generics = input.generics.clone();
    let type_params: Vec<_> = generics
        .params
        .iter()
        .filter_map(|param| match param {
            GenericParam::Type(type_param) => Some(type_param.ident.clone()),
            _ => None,
        })
        .collect();
    for ident in type_params {
        generics.make_where_clause().predicates.push(parse_quote! {
            #ident: Clone + ::serde::Serialize + ::serde::de::DeserializeOwned + Send + Sync + 'static
        });
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let expanded = quote! {
        impl #impl_generics ::delta_stream::schema::DeltaState for #name #ty_generics #where_clause {
            const SCHEMA_NAME: &'static str = stringify!(#name);
        }
    };
    TokenStream::from(expanded)
}

fn validate_input(input: &DeriveInput) -> syn::Result<()> {
    match &input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(_) => Ok(()),
            Fields::Unnamed(_) | Fields::Unit => Err(syn::Error::new_spanned(
                &input.ident,
                "DeltaState can only be derived for structs with named fields",
            )),
        },
        Data::Enum(_) | Data::Union(_) => Err(syn::Error::new_spanned(
            &input.ident,
            "DeltaState can only be derived for structs with named fields",
        )),
    }
}
